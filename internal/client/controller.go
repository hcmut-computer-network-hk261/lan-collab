// Package client holds the controller (all network logic), the console UI and the HTTP bridge for the browser UI.
package client

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"lancollab/internal/protocol"
	"lancollab/internal/transport"
)

// Session states (the client side of the session lifecycle FSM).
const (
	StateDisconnected   = "DISCONNECTED"
	StateTLSConnecting  = "TLS_CONNECTING"
	StateWaitHelloAck   = "WAIT_HELLO_ACK"
	StateAuthenticating = "AUTHENTICATING"
	StateOnline         = "ONLINE"
	StateClosing        = "CLOSING"
)

// Event kinds published by the controller.
const (
	EventState        = "state"
	EventPeers        = "peers"
	EventChat         = "chat"
	EventFileProgress = "file_progress"
	EventReceipt      = "receipt" // sent / delivered / seen for messages and files this user sent
	EventStats        = "stats"
	EventError        = "error"
)

// Transfer directions and stages.
const (
	DirectionSend = "send"
	DirectionRecv = "recv"

	StageSending         = "sending"
	StageAwaitingConfirm = "awaiting_confirmation"
	StageReceiving       = "receiving"
	StageVerified        = "verified"
	StageFailed          = "failed"
	StageQueued          = "queued" // stored on the server until the offline recipient signs in
)

// MaxChatTextBytes limits one chat message (UTF-8 bytes).
const MaxChatTextBytes = 512

const (
	dialTimeout          = 5 * time.Second
	handshakeStepTimeout = 5 * time.Second
	logoutEchoTimeout    = time.Second
	progressEmitInterval = 100 * time.Millisecond

	// DefaultTransferIdleTimeout fails an incoming transfer that receives no data for this long (safety net).
	DefaultTransferIdleTimeout = 30 * time.Second
	statsInterval              = time.Second
	historyLimit               = 200
	transferLimit              = 100
	subscriberBuffer           = 1024
)

// TransferProgress describes one file transfer, outgoing or incoming.
type TransferProgress struct {
	StreamID    uint16 `json:"stream_id"`
	PeerID      uint16 `json:"peer_id"`
	PeerName    string `json:"peer_name,omitempty"`
	Filename    string `json:"filename"`
	Direction   string `json:"direction"`
	Transferred int64  `json:"transferred"`
	Total       int64  `json:"total"`
	Percent     int    `json:"percent"`
	Verified    bool   `json:"verified"`
	Stage       string `json:"stage"`
	Error       string `json:"error,omitempty"`
	SavedPath   string `json:"saved_path,omitempty"`
	StartedAt   int64  `json:"started_at,omitempty"`
	MsgID       string `json:"msg_id,omitempty"`  // ID used by receipts
	Receipt     string `json:"receipt,omitempty"` // sent / delivered / seen (outgoing files only)
}

// ClientEvent is published to every subscriber (console, browser via SSE, tests).
type ClientEvent struct {
	Kind     string              `json:"kind"`
	State    string              `json:"state,omitempty"`
	UserID   uint16              `json:"user_id,omitempty"`
	Username string              `json:"username,omitempty"`
	Peers    []protocol.PeerInfo `json:"peers,omitempty"`
	From     string              `json:"from,omitempty"`
	FromID   uint16              `json:"from_id,omitempty"`
	TargetID uint16              `json:"target_id,omitempty"`
	Text     string              `json:"text,omitempty"`
	Direct   bool                `json:"direct,omitempty"`
	Self     bool                `json:"self,omitempty"`
	TS       int64               `json:"ts,omitempty"`
	Users    []string            `json:"users,omitempty"`   // every registered username, online or not
	Peer     string              `json:"peer,omitempty"`    // the other person's name in a direct conversation
	Stored   bool                `json:"stored,omitempty"`  // a message the server kept while this user was offline
	Queued   bool                `json:"queued,omitempty"`  // a message this user sent to someone offline
	ID       string              `json:"id,omitempty"`      // a direct message's ID
	Receipt  string              `json:"receipt,omitempty"` // latest status of a message this user sent
	IDs      []string            `json:"ids,omitempty"`     // messages or files a receipt event applies to
	Progress *TransferProgress   `json:"progress,omitempty"`
	Stats    *transport.RDTStats `json:"stats,omitempty"`
	Error    string              `json:"error,omitempty"`
}

// session holds everything that exists only while signed in.
type session struct {
	userID    uint16
	username  string
	key       []byte
	ctrl      *transport.TLSControlConn
	ep        *transport.RDTEndpoint
	serverUDP *net.UDPAddr
	heartbeat time.Duration
	reasm     *protocol.FileReassembler

	stop       chan struct{}
	stopOnce   sync.Once
	logoutAck  chan struct{}
	logoutOnce sync.Once

	chatMu  sync.Mutex // serialises stream-1 sends so sequence numbers leave in order
	chatSeq uint32

	streamMu   sync.Mutex
	nextStream uint16
}

// Controller is the client-side state machine; all exported methods are safe for concurrent use.
type Controller struct {
	serverHost string
	tcpPort    int
	udpPort    int

	loginMu sync.Mutex // serialises ConnectAndLogin and Logout

	// mu guards everything below; events are published under mu so subscribers see them in state order.
	mu            sync.Mutex
	state         string
	lastErr       string
	lastUser      string
	sess          *session
	peers         []protocol.PeerInfo
	users         []string // every registered username (online and offline)
	transfers     map[string]*TransferProgress
	transferOrder []string
	lastEmit      map[string]time.Time
	recvActivity  map[string]time.Time // last time each incoming transfer received data
	idleTimeout   time.Duration
	downloadDir   string // where received files are kept until the user saves them
	saveDir       string // where SaveReceived copies a file (console /save)
	fingerprint   string
	dropRate      float64
	subs          map[chan ClientEvent]struct{}
	history       []ClientEvent
	unseen        map[string][]string // peer name -> received message/file IDs not yet reported as seen
}

// NewController creates a disconnected controller for the given server.
func NewController(serverHost string, tcpPort, udpPort int) *Controller {
	return &Controller{
		serverHost:   serverHost,
		tcpPort:      tcpPort,
		udpPort:      udpPort,
		state:        StateDisconnected,
		transfers:    make(map[string]*TransferProgress),
		lastEmit:     make(map[string]time.Time),
		recvActivity: make(map[string]time.Time),
		idleTimeout:  DefaultTransferIdleTimeout,
		downloadDir:  "downloads",
		saveDir:      "downloads",
		subs:         make(map[chan ClientEvent]struct{}),
		unseen:       make(map[string][]string),
	}
}

// SetSaveDir sets where SaveReceived copies files (default "downloads").
func (c *Controller) SetSaveDir(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if dir != "" {
		c.saveDir = dir
	}
}

// SaveReceived copies a received file (by name, from the inbox) to the save folder and returns the new path.
func (c *Controller) SaveReceived(name string) (string, error) {
	name = filepath.Base(name)
	c.mu.Lock()
	inbox, dest := c.downloadDir, c.saveDir
	c.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(inbox, name))
	if err != nil {
		return "", fmt.Errorf("no received file called %q", name)
	}
	return saveDownload(dest, name, data)
}

// SetDownloadDir sets where received files are kept until the user saves them (default "downloads").
func (c *Controller) SetDownloadDir(dir string) {
	c.mu.Lock()
	if dir != "" {
		c.downloadDir = dir
	}
	c.mu.Unlock()
}

// SetTransferIdleTimeout sets the incoming-transfer idle timeout; d <= 0 disables it.
func (c *Controller) SetTransferIdleTimeout(d time.Duration) {
	c.mu.Lock()
	c.idleTimeout = d
	c.mu.Unlock()
}

// DownloadDir returns the directory where received files are saved.
func (c *Controller) DownloadDir() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.downloadDir
}

// DropRate returns the simulated outgoing loss rate (0 to 1).
func (c *Controller) DropRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropRate
}

// SetServerFingerprint pins the server's TLS certificate (hex SHA-256) for future logins.
func (c *Controller) SetServerFingerprint(fp string) {
	c.mu.Lock()
	c.fingerprint = fp
	c.mu.Unlock()
}

// SetDropRate injects simulated loss (0..1) into this client's outgoing UDP datagrams.
func (c *Controller) SetDropRate(rate float64) {
	c.mu.Lock()
	c.dropRate = rate
	s := c.sess
	c.mu.Unlock()
	if s != nil {
		s.ep.SetDropRate(rate)
	}
}

// ServerAddress returns "host:tcpPort".
func (c *Controller) ServerAddress() string {
	return net.JoinHostPort(c.serverHost, strconv.Itoa(c.tcpPort))
}

// ConnectAndLogin runs the handshake and sign-in (unknown names are registered) and starts the session goroutines.
func (c *Controller) ConnectAndLogin(username, password string) error {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()

	c.mu.Lock()
	if c.sess != nil {
		c.mu.Unlock()
		return errors.New("already signed in; sign out first")
	}
	fingerprint, dropRate := c.fingerprint, c.dropRate
	c.mu.Unlock()

	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return errors.New("enter a username and a password")
	}
	serverUDP, err := net.ResolveUDPAddr("udp", net.JoinHostPort(c.serverHost, strconv.Itoa(c.udpPort)))
	if err != nil {
		return fmt.Errorf("resolve server UDP address: %w", err)
	}

	c.setState(StateTLSConnecting, "")
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		c.setState(StateDisconnected, err.Error())
		return fmt.Errorf("open UDP socket: %w", err)
	}
	fail := func(err error) error {
		_ = udpConn.Close()
		c.setState(StateDisconnected, err.Error())
		return err
	}

	tcpAddr := c.ServerAddress()
	raw, err := tls.DialWithDialer(&net.Dialer{Timeout: dialTimeout}, "tcp", tcpAddr, protocol.NewPinnedClientTLSConfig(fingerprint))
	if err != nil {
		return fail(fmt.Errorf("cannot reach server %s: %w", tcpAddr, err))
	}
	ctrl := transport.NewTLSControlConn(raw)
	failConn := func(err error) error {
		_ = ctrl.Close()
		return fail(err)
	}

	localPort := udpConn.LocalAddr().(*net.UDPAddr).Port
	if err := ctrl.Send(protocol.ControlMsg{Type: protocol.CtrlHello, Version: int(protocol.Version), UDPPort: localPort}); err != nil {
		return failConn(fmt.Errorf("send HELLO: %w", err))
	}
	c.setState(StateWaitHelloAck, "")
	ack, err := ctrl.Recv(handshakeStepTimeout)
	if err != nil {
		return failConn(fmt.Errorf("waiting for HELLO_ACK: %w", err))
	}
	if ack.Type != protocol.CtrlHelloAck {
		return failConn(fmt.Errorf("server answered %s instead of HELLO_ACK: %s", ack.Type, ack.Reason))
	}
	heartbeat := protocol.HeartbeatInterval
	if ack.HeartbeatSec > 0 {
		heartbeat = time.Duration(ack.HeartbeatSec) * time.Second
	}

	c.setState(StateAuthenticating, "")
	if err := ctrl.Send(protocol.ControlMsg{Type: protocol.CtrlAuthReq, Username: username, Password: password}); err != nil {
		return failConn(fmt.Errorf("send AUTH_REQ: %w", err))
	}
	res, err := ctrl.Recv(handshakeStepTimeout)
	if err != nil {
		return failConn(fmt.Errorf("waiting for AUTH_RES: %w", err))
	}
	if res.Type != protocol.CtrlAuthRes {
		return failConn(fmt.Errorf("server answered %s instead of AUTH_RES: %s", res.Type, res.Reason))
	}
	if !res.OK {
		reason := res.Reason
		if reason == "" {
			reason = "sign-in refused by the server"
		}
		return failConn(errors.New(reason))
	}
	if res.UserID == protocol.ServerID || res.UserID == protocol.BroadcastTarget || len(res.SessionKey) != protocol.SessionKeySize {
		return failConn(errors.New("server sent an invalid AUTH_RES"))
	}

	s := &session{
		userID:     res.UserID,
		username:   username,
		key:        res.SessionKey,
		ctrl:       ctrl,
		serverUDP:  serverUDP,
		heartbeat:  heartbeat,
		reasm:      protocol.NewFileReassembler(),
		stop:       make(chan struct{}),
		logoutAck:  make(chan struct{}),
		chatSeq:    protocol.InitialSeq(protocol.StreamChat),
		nextStream: protocol.FirstFileStream,
	}
	// Every packet a client receives comes from the server, so it is always protected by this client's own key.
	s.ep = transport.NewRDTEndpoint(udpConn, func(uint16) ([]byte, *net.UDPAddr, bool) {
		return s.key, s.serverUDP, true
	})
	s.ep.SetLocalID(s.userID)
	if dropRate > 0 {
		s.ep.SetDropRate(dropRate)
	}

	c.mu.Lock()
	if c.lastUser != username {
		// A different user signed in on this client: hide the previous user's conversations and transfers.
		c.history = nil
		c.transfers = make(map[string]*TransferProgress)
		c.transferOrder = nil
		c.lastEmit = make(map[string]time.Time)
		c.recvActivity = make(map[string]time.Time)
		c.unseen = make(map[string][]string)
	}
	c.lastUser = username
	c.sess = s
	c.state = StateOnline
	c.lastErr = ""
	c.peers = sortedPeers(res.Peers)
	c.users = append([]string(nil), res.Users...)
	c.emitLocked(ClientEvent{Kind: EventState, State: StateOnline, UserID: s.userID, Username: s.username})
	c.emitLocked(c.peersEventLocked())
	c.mu.Unlock()

	go c.heartbeatLoop(s)
	go c.controlLoop(s)
	go c.chatLoop(s)
	go c.fileLoop(s)
	go c.statsLoop(s)
	return nil
}

// Logout sends LOGOUT, waits up to 1 s for the echo, and closes both sockets.
func (c *Controller) Logout() error {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()

	c.mu.Lock()
	s := c.sess
	if s == nil {
		c.mu.Unlock()
		return nil
	}
	c.state = StateClosing
	c.emitLocked(ClientEvent{Kind: EventState, State: StateClosing, UserID: s.userID, Username: s.username})
	c.mu.Unlock()

	if err := s.ctrl.Send(protocol.ControlMsg{Type: protocol.CtrlLogout}); err == nil {
		select {
		case <-s.logoutAck:
		case <-s.stop:
		case <-time.After(logoutEchoTimeout):
		}
	}
	c.teardown(s, "")
	return nil
}

// SendChat sends text to everyone (targetID = protocol.BroadcastTarget) or to one user.
func (c *Controller) SendChat(targetID uint16, text string) error {
	s, err := c.currentSession()
	if err != nil {
		return err
	}
	text, err = checkChatText(text)
	if err != nil {
		return err
	}
	switch {
	case targetID == protocol.ServerID || targetID == s.userID:
		return errors.New("choose another user or everyone as the recipient")
	case targetID != protocol.BroadcastTarget && !c.peerOnline(targetID):
		return fmt.Errorf("user #%d is not online", targetID)
	}
	ts := time.Now().UnixMilli()
	id := ""
	if targetID != protocol.BroadcastTarget {
		id = newMsgID()
	}
	body, err := protocol.MarshalJSONCompact(protocol.ChatPayload{From: s.username, Text: text, TS: ts, ID: id})
	if err != nil {
		return err
	}
	if len(body) > protocol.MaxChunkSize {
		return errors.New("the message is too long once encoded")
	}
	if err := c.sendOnChatStream(s, protocol.MsgChat, targetID, body); err != nil {
		return err
	}
	c.mu.Lock()
	peer := ""
	if targetID != protocol.BroadcastTarget {
		peer = c.peerNameLocked(targetID)
	}
	c.emitLocked(ClientEvent{
		Kind: EventChat, Self: true, From: s.username, FromID: s.userID, TargetID: targetID, Peer: peer,
		Text: text, Direct: targetID != protocol.BroadcastTarget, TS: ts, ID: id,
	})
	c.mu.Unlock()
	return nil
}

// SendChatTo sends a direct message by username; an offline user gets it from the server when they sign in.
func (c *Controller) SendChatTo(name, text string) error {
	s, err := c.currentSession()
	if err != nil {
		return err
	}
	c.mu.Lock()
	id, online := c.peerIDByNameLocked(name)
	known := c.knownUserLocked(name)
	c.mu.Unlock()
	switch {
	case name == s.username:
		return errors.New("choose another user or everyone as the recipient")
	case online:
		return c.SendChat(id, text)
	case !known:
		return fmt.Errorf("there is no user called %q", name)
	}
	text, err = checkChatText(text)
	if err != nil {
		return err
	}
	ts := time.Now().UnixMilli()
	msgID := newMsgID()
	body, err := protocol.MarshalJSONCompact(protocol.ChatPayload{From: s.username, Text: text, TS: ts, To: name, ID: msgID})
	if err != nil {
		return err
	}
	if len(body) > protocol.MaxChunkSize {
		return errors.New("the message is too long once encoded")
	}
	if err := c.sendOnChatStream(s, protocol.MsgChat, protocol.ServerID, body); err != nil {
		return err
	}
	c.mu.Lock()
	c.emitLocked(ClientEvent{
		Kind: EventChat, Self: true, From: s.username, FromID: s.userID, Peer: name,
		Text: text, Direct: true, Queued: true, TS: ts, ID: msgID,
	})
	c.mu.Unlock()
	return nil
}

// checkChatText trims a chat message and rejects empty, oversized or invalid text.
func checkChatText(text string) (string, error) {
	text = strings.TrimSpace(text)
	switch {
	case text == "":
		return "", errors.New("the message is empty")
	case len(text) > MaxChatTextBytes:
		return "", fmt.Errorf("the message is longer than %d bytes", MaxChatTextBytes)
	case !utf8.ValidString(text):
		return "", errors.New("the message is not valid UTF-8")
	}
	return text, nil
}

// SendFile starts a background transfer to targetID on a new stream and returns that stream ID.
func (c *Controller) SendFile(targetID uint16, filename string, data []byte) (uint16, error) {
	s, err := c.currentSession()
	if err != nil {
		return 0, err
	}
	if targetID == protocol.ServerID || targetID == protocol.BroadcastTarget || targetID == s.userID {
		return 0, errors.New("files can only be sent to one other user")
	}
	if !c.peerOnline(targetID) {
		return 0, fmt.Errorf("user #%d is not online", targetID)
	}
	c.mu.Lock()
	name := c.peerNameLocked(targetID)
	c.mu.Unlock()
	return c.startUpload(s, targetID, name, "", filename, data)
}

// SendFileTo sends a file by username; for an offline user the server stores it and delivers it on sign-in.
func (c *Controller) SendFileTo(name, filename string, data []byte) (uint16, error) {
	s, err := c.currentSession()
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	id, online := c.peerIDByNameLocked(name)
	known := c.knownUserLocked(name)
	c.mu.Unlock()
	switch {
	case name == s.username:
		return 0, errors.New("files can only be sent to one other user")
	case online:
		return c.SendFile(id, filename, data)
	case !known:
		return 0, fmt.Errorf("there is no user called %q", name)
	}
	return c.startUpload(s, protocol.ServerID, name, name, filename, data)
}

// startUpload registers a send transfer and uploads it in the background; target 0 with to set means "store for to".
func (c *Controller) startUpload(s *session, target uint16, peerName, to, filename string, data []byte) (uint16, error) {
	if int64(len(data)) > protocol.MaxFileSize {
		return 0, fmt.Errorf("the file is larger than %d MiB", protocol.MaxFileSize>>20)
	}
	name := sanitizeFilename(filename)
	meta, chunks := protocol.SplitFileChunks(data)
	meta.Filename = name
	meta.To = to
	meta.ID = newMsgID()
	initBody, err := protocol.MarshalJSONCompact(meta)
	if err != nil {
		return 0, err
	}
	stream := s.allocStream()
	key := transferKey(DirectionSend, s.userID, stream)

	c.mu.Lock()
	c.putTransferLocked(key, &TransferProgress{
		StreamID: stream, PeerID: target, PeerName: peerName,
		Filename: name, Direction: DirectionSend, Total: meta.Size, Stage: StageSending, MsgID: meta.ID,
	})
	c.mu.Unlock()

	go c.runUpload(s, key, target, stream, initBody, chunks)
	return stream, nil
}

// SubscribeEvents replays the current state to a new subscriber, then streams events (slow subscribers lose events).
func (c *Controller) SubscribeEvents() <-chan ClientEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan ClientEvent, subscriberBuffer)
	ch <- c.stateEventLocked()
	ch <- c.peersEventLocked()
	for _, ev := range c.history {
		ch <- ev
	}
	for _, key := range c.transferOrder {
		if tp := c.transfers[key]; tp != nil {
			cp := *tp
			ch <- ClientEvent{Kind: EventFileProgress, Progress: &cp}
		}
	}
	c.subs[ch] = struct{}{}
	return ch
}

// UnsubscribeEvents stops delivery to ch and closes it.
func (c *Controller) UnsubscribeEvents(ch <-chan ClientEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for sub := range c.subs {
		if (<-chan ClientEvent)(sub) == ch {
			delete(c.subs, sub)
			close(sub)
			return
		}
	}
}

// Snapshot returns the whole client state as JSON-friendly values.
func (c *Controller) Snapshot() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	transfers := make([]TransferProgress, 0, len(c.transferOrder))
	for _, key := range c.transferOrder {
		if tp := c.transfers[key]; tp != nil {
			transfers = append(transfers, *tp)
		}
	}
	chats := make([]ClientEvent, len(c.history))
	copy(chats, c.history)
	snap := map[string]any{
		"state":      c.state,
		"server":     c.ServerAddress(),
		"peers":      copyPeers(c.peers),
		"users":      append([]string(nil), c.users...),
		"transfers":  transfers,
		"chats":      chats,
		"last_error": c.lastErr,
		"drop_rate":  c.dropRate,
	}
	if c.sess != nil {
		snap["user_id"] = c.sess.userID
		snap["username"] = c.sess.username
		snap["stats"] = c.sess.ep.Stats(protocol.ServerID)
	}
	return snap
}

// State returns the current FSM state.
func (c *Controller) State() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// UserID returns the UserID assigned by the server, or 0 when signed out.
func (c *Controller) UserID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil {
		return 0
	}
	return c.sess.userID
}

// Username returns the signed-in username, or "".
func (c *Controller) Username() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil {
		return ""
	}
	return c.sess.username
}

// Peers returns the current online-user list.
func (c *Controller) Peers() []protocol.PeerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyPeers(c.peers)
}

// Users returns every registered username, online or offline.
func (c *Controller) Users() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.users...)
}

// peersEventLocked builds the peers event with the online list and all usernames; c.mu must be held.
func (c *Controller) peersEventLocked() ClientEvent {
	return ClientEvent{Kind: EventPeers, Peers: copyPeers(c.peers), Users: append([]string(nil), c.users...)}
}

// peerIDByNameLocked returns the UserID of name if that user is online; c.mu must be held.
func (c *Controller) peerIDByNameLocked(name string) (uint16, bool) {
	for _, p := range c.peers {
		if p.Name == name {
			return p.ID, true
		}
	}
	return 0, false
}

// knownUserLocked reports whether name is a registered user; c.mu must be held.
func (c *Controller) knownUserLocked(name string) bool {
	for _, u := range c.users {
		if u == name {
			return true
		}
	}
	return false
}

// Transfers returns all known transfers, oldest first.
func (c *Controller) Transfers() []TransferProgress {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TransferProgress, 0, len(c.transferOrder))
	for _, key := range c.transferOrder {
		if tp := c.transfers[key]; tp != nil {
			out = append(out, *tp)
		}
	}
	return out
}

// Stats returns transport statistics toward the server while signed in.
func (c *Controller) Stats() (transport.RDTStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil {
		return transport.RDTStats{}, false
	}
	return c.sess.ep.Stats(protocol.ServerID), true
}

// --- Background goroutines (one set per session) ---

func (c *Controller) heartbeatLoop(s *session) {
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	send := func() {
		_ = s.ep.SendUnreliable(s.serverUDP, s.key, protocol.UDPHeader{
			MsgType:  protocol.MsgHeartbeat,
			StreamID: protocol.StreamHeartbeat,
			SenderID: s.userID,
			TargetID: protocol.ServerID,
		}, nil)
	}
	send() // tell the server our real UDP address right away
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			send()
		}
	}
}

func (c *Controller) controlLoop(s *session) {
	for {
		msg, err := s.ctrl.Recv(0)
		if err != nil {
			c.teardown(s, "lost connection to the server: "+err.Error())
			return
		}
		switch msg.Type {
		case protocol.CtrlPeerUpdate:
			var failed []TransferProgress
			c.mu.Lock()
			if c.sess == s {
				c.peers = sortedPeers(msg.Peers)
				if msg.Users != nil {
					c.users = append([]string(nil), msg.Users...)
				}
				c.emitLocked(c.peersEventLocked())
				failed = c.failOfflineTransfersLocked()
			}
			c.mu.Unlock()
			// The reassembler has its own mutex: touch it only after releasing c.mu.
			for _, tp := range failed {
				if tp.Direction == DirectionRecv {
					s.reasm.Abort(tp.PeerID, tp.StreamID)
				}
			}
		case protocol.CtrlLogout:
			s.logoutOnce.Do(func() { close(s.logoutAck) })
		case protocol.CtrlError:
			c.mu.Lock()
			c.emitLocked(ClientEvent{Kind: EventError, Error: msg.Reason})
			c.mu.Unlock()
		}
	}
}

func (c *Controller) chatLoop(s *session) {
	for pkt := range s.ep.ChatChan() {
		h := pkt.Header
		switch h.MsgType {
		case protocol.MsgChat:
			var chat protocol.ChatPayload
			if err := json.Unmarshal(pkt.Plaintext, &chat); err != nil {
				continue
			}
			direct := h.TargetID != protocol.BroadcastTarget
			peer := ""
			if direct {
				peer = chat.From
			}
			c.mu.Lock()
			c.emitLocked(ClientEvent{
				Kind: EventChat, From: chat.From, FromID: h.SenderID, TargetID: h.TargetID, Peer: peer,
				Text: chat.Text, Direct: direct, Stored: chat.Stored, TS: chat.TS, ID: chat.ID,
			})
			c.mu.Unlock()
			if direct {
				c.noteReceived(s, peer, chat.ID)
			}
		case protocol.MsgReceipt:
			c.onReceipt(pkt.Plaintext)
		case protocol.MsgFileDone:
			var done protocol.FileDonePayload
			if err := json.Unmarshal(pkt.Plaintext, &done); err != nil {
				continue
			}
			key := transferKey(DirectionSend, s.userID, done.StreamID)
			c.updateTransfer(key, true, func(tp *TransferProgress) {
				tp.Verified = done.OK && !done.Queued
				if done.OK && done.Queued {
					tp.Stage = StageQueued // stored on the server; delivered when the recipient signs in
					tp.Transferred, tp.Percent = tp.Total, 100
					tp.Receipt = upgradeReceipt(tp.Receipt, protocol.ReceiptSent)
				} else if done.OK {
					tp.Stage = StageVerified
					tp.Transferred, tp.Percent = tp.Total, 100
					tp.Receipt = upgradeReceipt(tp.Receipt, protocol.ReceiptDelivered)
				} else {
					tp.Stage = StageFailed
					tp.Error = done.Reason
				}
			})
		}
	}
}

func (c *Controller) fileLoop(s *session) {
	for pkt := range s.ep.FileChan() {
		h := pkt.Header
		switch h.MsgType {
		case protocol.MsgFileInit:
			c.onFileInit(s, h, pkt.Plaintext)
		case protocol.MsgFileChunk:
			c.onFileChunk(s, h, pkt.Plaintext)
		case protocol.MsgFileEnd:
			c.onFileEnd(s, h)
		}
	}
}

func (c *Controller) statsLoop(s *session) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			st := s.ep.Stats(protocol.ServerID)
			c.mu.Lock()
			if c.sess == s {
				c.emitLocked(ClientEvent{Kind: EventStats, Stats: &st})
			}
			c.mu.Unlock()
			c.expireIdleTransfers(s)
		}
	}
}

// --- Sending on stream 1 ---

// sendOnChatStream sends one reliable stream-1 message (CHAT_MSG or FILE_DONE).
func (c *Controller) sendOnChatStream(s *session, msgType uint8, target uint16, body []byte) error {
	s.chatMu.Lock()
	defer s.chatMu.Unlock()
	flags := protocol.FlagEOM
	if target == protocol.BroadcastTarget {
		flags |= protocol.FlagBcast
	}
	hdr := protocol.UDPHeader{
		MsgType:  msgType,
		Flags:    flags,
		StreamID: protocol.StreamChat,
		SenderID: s.userID,
		TargetID: target,
		SeqNum:   s.chatSeq,
	}
	if err := s.ep.SendReliable(protocol.ServerID, s.serverUDP, s.key, hdr, body); err != nil {
		return err
	}
	s.chatSeq++
	return nil
}

// --- State helpers ---

// teardown closes one session exactly once (DISCONNECTED in the FSM).
func (c *Controller) teardown(s *session, reason string) {
	first := false
	s.stopOnce.Do(func() {
		first = true
		close(s.stop)
		_ = s.ctrl.Close()
		_ = s.ep.Close() // also closes the UDP socket and the delivery channels
	})
	if !first {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess != s {
		return
	}
	userInitiated := c.state == StateClosing
	c.sess = nil
	c.state = StateDisconnected
	if !userInitiated {
		c.lastErr = reason
	}
	for _, key := range c.transferOrder {
		tp := c.transfers[key]
		if tp != nil && (tp.Stage == StageSending || tp.Stage == StageReceiving || tp.Stage == StageAwaitingConfirm) {
			tp.Stage = StageFailed
			tp.Error = "disconnected before the transfer finished"
			cp := *tp
			c.emitLocked(ClientEvent{Kind: EventFileProgress, Progress: &cp})
		}
	}
	ev := ClientEvent{Kind: EventState, State: StateDisconnected}
	if !userInitiated {
		ev.Error = reason
	}
	c.emitLocked(ev)
}

func (c *Controller) currentSession() (*session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == nil || c.state != StateOnline {
		return nil, errors.New("not signed in")
	}
	return c.sess, nil
}

func (c *Controller) setState(state, errText string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
	if errText != "" {
		c.lastErr = errText
	}
	ev := c.stateEventLocked()
	ev.Error = errText
	c.emitLocked(ev)
}

func (c *Controller) stateEventLocked() ClientEvent {
	ev := ClientEvent{Kind: EventState, State: c.state, Error: c.lastErr}
	if c.sess != nil {
		ev.UserID = c.sess.userID
		ev.Username = c.sess.username
	}
	return ev
}

func (c *Controller) peerOnline(id uint16) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerOnlineLocked(id)
}

// peerOnlineLocked reports whether id is in the current peer list. c.mu must be held.
func (c *Controller) peerOnlineLocked(id uint16) bool {
	for _, p := range c.peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

func (c *Controller) peerNameLocked(id uint16) string {
	for _, p := range c.peers {
		if p.ID == id {
			return p.Name
		}
	}
	return ""
}

// emitLocked records chat history and fans ev out to subscribers without blocking; c.mu must be held.
func (c *Controller) emitLocked(ev ClientEvent) {
	if ev.Kind == EventChat {
		c.history = append(c.history, ev)
		if len(c.history) > historyLimit {
			c.history = append([]ClientEvent(nil), c.history[len(c.history)-historyLimit:]...)
		}
	}
	for ch := range c.subs {
		select {
		case ch <- ev:
		default: // subscriber is too slow; drop rather than stall the network goroutines
		}
	}
}

func sortedPeers(in []protocol.PeerInfo) []protocol.PeerInfo {
	out := copyPeers(in)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func copyPeers(in []protocol.PeerInfo) []protocol.PeerInfo {
	out := make([]protocol.PeerInfo, len(in))
	copy(out, in)
	return out
}
