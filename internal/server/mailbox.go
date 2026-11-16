package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"lancollab/internal/protocol"
	"lancollab/internal/transport"
)

// Offline delivery limits, per recipient.
const (
	mailboxChatLimit    = 200                // stored chat messages; the oldest is dropped when full
	mailboxReceiptLimit = 500                // stored receipts for a sender who is offline
	mailboxFileLimit    = 50                 // stored files
	mailboxBytesLimit   = 256 << 20          // bytes of stored files, including uploads in progress
	mailboxTTL          = 7 * 24 * time.Hour // stored messages and files expire after this
	spoolIdleTimeout    = 60 * time.Second   // an upload to the spool with no data for this long is aborted
	maxDeliveryAttempts = 3                  // a stored file the recipient rejects this often is dropped
)

// storedChat is one chat message kept for an offline user (JSON body ready to send).
type storedChat struct {
	body []byte
	at   time.Time
}

// storedFile is one complete, verified file on disk, waiting for its recipient.
type storedFile struct {
	from       string                // sender's username
	fromID     uint16                // sender's session when it was sent, to report delivery
	fromStream uint16                // sender's stream, to report delivery
	meta       protocol.FileInitMeta // To cleared, From set
	path       string
	at         time.Time
	inflight   bool // being delivered right now
	attempts   int
}

// userBox is everything waiting for one username.
type userBox struct {
	chats    []storedChat
	receipts []storedChat // RECEIPT bodies for this user, who sent the messages
	files    []*storedFile
	bytes    int64 // stored files plus uploads in progress
}

// spoolUpload is a file being received from a sender for an offline user, written straight to disk.
type spoolUpload struct {
	to      string
	meta    protocol.FileInitMeta
	f       *os.File
	hash    hash.Hash
	next    uint32 // next expected sequence number
	written int64
	last    time.Time
}

// deliveryKey identifies a stored file being sent to a recipient on one of the server's own streams.
type deliveryKey struct {
	recipient uint16
	stream    uint16
}

// mailbox keeps offline chat in memory and offline files in a spool directory.
type mailbox struct {
	mu         sync.Mutex
	dir        string
	boxes      map[string]*userBox
	uploads    map[fileKey]*spoolUpload
	deliveries map[deliveryKey]*storedFile
	chatLimit  int
	fileLimit  int
	byteLimit  int64
	ttl        time.Duration
}

func newMailbox(dir string) *mailbox {
	return &mailbox{
		dir:        dir,
		boxes:      make(map[string]*userBox),
		uploads:    make(map[fileKey]*spoolUpload),
		deliveries: make(map[deliveryKey]*storedFile),
		chatLimit:  mailboxChatLimit,
		fileLimit:  mailboxFileLimit,
		byteLimit:  mailboxBytesLimit,
		ttl:        mailboxTTL,
	}
}

// boxLocked returns (creating if needed) the box of name; m.mu must be held.
func (m *mailbox) boxLocked(name string) *userBox {
	b := m.boxes[name]
	if b == nil {
		b = &userBox{}
		m.boxes[name] = b
	}
	return b
}

// addChat stores a chat message for name, dropping the oldest when the box is full.
func (m *mailbox) addChat(name string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxLocked(name)
	if len(b.chats) >= m.chatLimit {
		b.chats = b.chats[1:]
	}
	b.chats = append(b.chats, storedChat{body: body, at: time.Now()})
}

// addReceipt stores a receipt for name, dropping the oldest when the box is full.
func (m *mailbox) addReceipt(name string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxLocked(name)
	if len(b.receipts) >= mailboxReceiptLimit {
		b.receipts = b.receipts[1:]
	}
	b.receipts = append(b.receipts, storedChat{body: body, at: time.Now()})
}

// takeReceipts removes and returns every receipt stored for name.
func (m *mailbox) takeReceipts(name string) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxes[name]
	if b == nil {
		return nil
	}
	out := make([][]byte, 0, len(b.receipts))
	for _, c := range b.receipts {
		out = append(out, c.body)
	}
	b.receipts = nil
	return out
}

// takeChats removes and returns every chat message stored for name.
func (m *mailbox) takeChats(name string) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxes[name]
	if b == nil {
		return nil
	}
	out := make([][]byte, 0, len(b.chats))
	for _, c := range b.chats {
		out = append(out, c.body)
	}
	b.chats = nil
	return out
}

// pendingFiles reports whether name has stored files that are not being delivered.
func (m *mailbox) pendingFiles(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.boxes[name]; b != nil {
		for _, f := range b.files {
			if !f.inflight {
				return true
			}
		}
	}
	return false
}

// startUpload reserves space and opens a spool file for a new upload.
func (m *mailbox) startUpload(fk fileKey, to string, meta protocol.FileInitMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxLocked(to)
	files := len(b.files)
	for k, up := range m.uploads {
		if up.to == to && k != fk {
			files++
		}
	}
	if files >= m.fileLimit || b.bytes+meta.Size > m.byteLimit {
		return fmt.Errorf("%s's offline mailbox is full", to)
	}
	f, err := os.CreateTemp(m.dir, "lc-*.part")
	if err != nil {
		return errors.New("the server could not store the file")
	}
	b.bytes += meta.Size
	m.uploads[fk] = &spoolUpload{to: to, meta: meta, f: f, hash: sha256.New(), next: 1, last: time.Now()}
	return nil
}

// writeChunk appends one in-order chunk of an upload.
func (m *mailbox) writeChunk(fk fileKey, seq uint32, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	up := m.uploads[fk]
	if up == nil {
		return nil // already finished or aborted
	}
	if seq != up.next || up.written+int64(len(data)) > up.meta.Size {
		return errors.New("the upload to the server was out of order or too long")
	}
	if _, err := up.f.Write(data); err != nil {
		return errors.New("the server could not store the file")
	}
	up.hash.Write(data)
	up.written += int64(len(data))
	up.next++
	up.last = time.Now()
	return nil
}

// finishUpload verifies a completed upload and moves it into the recipient's box.
func (m *mailbox) finishUpload(fk fileKey, from string, fromID uint16) (to string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up := m.uploads[fk]
	if up == nil {
		return "", errors.New("unknown upload")
	}
	delete(m.uploads, fk)
	partPath := up.f.Name()
	closeErr := up.f.Close()
	ok := closeErr == nil && up.next == up.meta.TotalChunks+1 && up.written == up.meta.Size &&
		hex.EncodeToString(up.hash.Sum(nil)) == up.meta.SHA256
	if !ok {
		_ = os.Remove(partPath)
		m.boxLocked(up.to).bytes -= up.meta.Size
		return up.to, errors.New("the upload to the server was incomplete or corrupted")
	}
	finalPath := partPath[:len(partPath)-len(".part")] + ".bin"
	if err := os.Rename(partPath, finalPath); err != nil {
		_ = os.Remove(partPath)
		m.boxLocked(up.to).bytes -= up.meta.Size
		return up.to, errors.New("the server could not store the file")
	}
	meta := up.meta
	meta.To, meta.From = "", from
	b := m.boxLocked(up.to)
	b.files = append(b.files, &storedFile{from: from, fromID: fromID, fromStream: fk.stream, meta: meta, path: finalPath, at: time.Now()})
	return up.to, nil
}

// abortUploadLocked deletes an unfinished upload and releases its space; m.mu must be held.
func (m *mailbox) abortUploadLocked(fk fileKey) {
	up := m.uploads[fk]
	if up == nil {
		return
	}
	delete(m.uploads, fk)
	_ = up.f.Close()
	_ = os.Remove(up.f.Name())
	m.boxLocked(up.to).bytes -= up.meta.Size
}

func (m *mailbox) abortUpload(fk fileKey) {
	m.mu.Lock()
	m.abortUploadLocked(fk)
	m.mu.Unlock()
}

// forgetSession aborts uploads from userID and releases files it was receiving, so they are retried later.
func (m *mailbox) forgetSession(userID uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for fk := range m.uploads {
		if fk.sender == userID {
			m.abortUploadLocked(fk)
		}
	}
	for dk, f := range m.deliveries {
		if dk.recipient == userID {
			f.inflight = false
			delete(m.deliveries, dk)
		}
	}
}

// claimFile marks the next waiting file of name as in flight on stream and returns it.
func (m *mailbox) claimFile(name string, recipient, stream uint16) *storedFile {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.boxes[name]
	if b == nil {
		return nil
	}
	for _, f := range b.files {
		if !f.inflight {
			f.inflight = true
			m.deliveries[deliveryKey{recipient: recipient, stream: stream}] = f
			return f
		}
	}
	return nil
}

// releaseFile puts an interrupted delivery back in the queue.
func (m *mailbox) releaseFile(recipient, stream uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dk := deliveryKey{recipient: recipient, stream: stream}
	if f := m.deliveries[dk]; f != nil {
		f.inflight = false
		delete(m.deliveries, dk)
	}
}

// delivered handles the recipient's FILE_DONE for a stored file; it returns the file and whether it is now gone.
func (m *mailbox) delivered(recipient, stream uint16, ok bool) (f *storedFile, removed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dk := deliveryKey{recipient: recipient, stream: stream}
	f = m.deliveries[dk]
	if f == nil {
		return nil, false
	}
	delete(m.deliveries, dk)
	f.inflight = false
	if !ok {
		f.attempts++
		if f.attempts < maxDeliveryAttempts {
			return f, false
		}
	}
	m.removeFileLocked(f)
	return f, true
}

// removeFileLocked deletes a stored file from its box and from disk; m.mu must be held.
func (m *mailbox) removeFileLocked(f *storedFile) {
	for name, b := range m.boxes {
		for i, g := range b.files {
			if g == f {
				b.files = append(b.files[:i], b.files[i+1:]...)
				b.bytes -= f.meta.Size
				_ = os.Remove(f.path)
				if len(b.files) == 0 && len(b.chats) == 0 && b.bytes == 0 {
					delete(m.boxes, name)
				}
				return
			}
		}
	}
}

// discard deletes a stored file that can no longer be read.
func (m *mailbox) discard(f *storedFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for dk, g := range m.deliveries {
		if g == f {
			delete(m.deliveries, dk)
		}
	}
	m.removeFileLocked(f)
}

// sweep aborts idle uploads and deletes expired messages and files; it returns the aborted uploads.
func (m *mailbox) sweep(now time.Time) []fileKey {
	m.mu.Lock()
	defer m.mu.Unlock()
	var aborted []fileKey
	for fk, up := range m.uploads {
		if now.Sub(up.last) > spoolIdleTimeout {
			m.abortUploadLocked(fk)
			aborted = append(aborted, fk)
		}
	}
	for _, b := range m.boxes {
		kept := b.chats[:0]
		for _, c := range b.chats {
			if now.Sub(c.at) <= m.ttl {
				kept = append(kept, c)
			}
		}
		b.chats = kept
		keptR := b.receipts[:0]
		for _, c := range b.receipts {
			if now.Sub(c.at) <= m.ttl {
				keptR = append(keptR, c)
			}
		}
		b.receipts = keptR
		for _, f := range append([]*storedFile(nil), b.files...) {
			if !f.inflight && now.Sub(f.at) > m.ttl {
				m.removeFileLocked(f)
			}
		}
	}
	return aborted
}

// close aborts every upload (server shutdown).
func (m *mailbox) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for fk := range m.uploads {
		m.abortUploadLocked(fk)
	}
}

// --- Router side: storing, delivering and reporting --------------------------------------

// storeOrForwardChat handles a chat sent to the server for a named user: forward if online, else store.
func (r *Router) storeOrForwardChat(senderID uint16, senderName string, chat protocol.ChatPayload) {
	to := chat.To
	if to == "" || to == senderName || !r.sm.HasUser(to) {
		r.logf("dropping chat from #%d to unknown user %q", senderID, to)
		return
	}
	chat.To = ""
	if id, online := r.sm.IDByName(to); online {
		if body, err := protocol.MarshalJSONCompact(chat); err == nil &&
			r.enqueueChatStream(senderID, id, protocol.MsgChat, protocol.FlagEOM, id, body) {
			r.sendReceipt(senderID, protocol.ReceiptSent, to, chat.ID)
		}
		return
	}
	chat.Stored = true
	body, err := protocol.MarshalJSONCompact(chat)
	if err != nil || len(body) > protocol.MaxChunkSize {
		return
	}
	r.box.addChat(to, body)
	r.sendReceipt(senderID, protocol.ReceiptSent, to, chat.ID)
}

// sendReceipt tells a sender that the server has their message (status "sent").
func (r *Router) sendReceipt(to uint16, status, peer, id string) {
	if id == "" {
		return
	}
	body, err := protocol.MarshalJSONCompact(protocol.ReceiptPayload{Status: status, IDs: []string{id}, From: peer})
	if err != nil {
		return
	}
	r.enqueueChatStream(protocol.ServerID, to, protocol.MsgReceipt, protocol.FlagEOM, to, body)
}

// relayReceipt forwards a recipient's delivered/seen receipt to the original sender, or stores it if they are offline.
func (r *Router) relayReceipt(senderID uint16, senderName string, plaintext []byte) {
	var rc protocol.ReceiptPayload
	if err := json.Unmarshal(plaintext, &rc); err != nil {
		return
	}
	if rc.Status != protocol.ReceiptDelivered && rc.Status != protocol.ReceiptSeen {
		return
	}
	if len(rc.IDs) == 0 || len(rc.IDs) > protocol.MaxReceiptIDs || rc.To == "" || rc.To == senderName || !r.sm.HasUser(rc.To) {
		return
	}
	for _, id := range rc.IDs {
		if id == "" || len(id) > 32 {
			return
		}
	}
	to := rc.To
	rc.To, rc.From = "", senderName // the server vouches for who saw it
	body, err := protocol.MarshalJSONCompact(rc)
	if err != nil || len(body) > protocol.MaxChunkSize {
		return
	}
	if id, online := r.sm.IDByName(to); online && r.enqueueChatStream(protocol.ServerID, id, protocol.MsgReceipt, protocol.FlagEOM, id, body) {
		return
	}
	r.box.addReceipt(to, body)
}

// spoolFile receives a file sent to the server for an offline user and writes it to disk.
func (r *Router) spoolFile(pkt transport.DeliveredPacket) {
	h := pkt.Header
	fk := fileKey{sender: h.SenderID, stream: h.StreamID}
	r.mu.Lock()
	_, failed := r.failedFiles[fk]
	r.mu.Unlock()
	if failed {
		return
	}
	switch h.MsgType {
	case protocol.MsgFileInit:
		var meta protocol.FileInitMeta
		if err := json.Unmarshal(pkt.Plaintext, &meta); err != nil || meta.Validate() != nil {
			r.failSpool(h, "invalid FILE_INIT")
			return
		}
		sender, ok := r.sm.Get(h.SenderID)
		if !ok {
			return
		}
		if meta.To == "" || meta.To == sender.Username || !r.sm.HasUser(meta.To) {
			r.failSpool(h, "unknown recipient")
			return
		}
		if err := r.box.startUpload(fk, meta.To, meta); err != nil {
			r.failSpool(h, err.Error())
		}
	case protocol.MsgFileChunk:
		if err := r.box.writeChunk(fk, h.SeqNum, pkt.Plaintext); err != nil {
			r.failSpool(h, err.Error())
		}
	case protocol.MsgFileEnd:
		sender, ok := r.sm.Get(h.SenderID)
		if !ok {
			r.box.abortUpload(fk)
			return
		}
		to, err := r.box.finishUpload(fk, sender.Username, h.SenderID)
		if err != nil {
			r.failSpool(h, err.Error())
			return
		}
		r.logf("stored file #%d from %q for offline user %q", h.StreamID, sender.Username, to)
		r.sendFileDone(h.SenderID, protocol.FileDonePayload{StreamID: h.StreamID, OK: true, Queued: true})
		if id, online := r.sm.IDByName(to); online {
			r.kickDelivery(id)
		}
	}
}

// failSpool aborts a spool upload and tells its sender once.
func (r *Router) failSpool(h protocol.UDPHeader, reason string) {
	fk := fileKey{sender: h.SenderID, stream: h.StreamID}
	r.mu.Lock()
	if _, done := r.failedFiles[fk]; done {
		r.mu.Unlock()
		return
	}
	r.failedFiles[fk] = struct{}{}
	r.mu.Unlock()
	r.box.abortUpload(fk)
	r.logf("offline upload #%d from #%d failed: %s", h.StreamID, h.SenderID, reason)
	r.sendFileDone(h.SenderID, protocol.FileDonePayload{StreamID: h.StreamID, OK: false, Reason: reason})
}

// sendFileDone sends a FILE_DONE from the server itself to a user.
func (r *Router) sendFileDone(to uint16, done protocol.FileDonePayload) {
	body, err := protocol.MarshalJSONCompact(done)
	if err != nil {
		return
	}
	r.enqueueChatStream(protocol.ServerID, to, protocol.MsgFileDone, protocol.FlagEOM, to, body)
}

// storedFileDone handles a recipient's verdict on a stored file and tells the original sender if still online.
func (r *Router) storedFileDone(recipient uint16, done protocol.FileDonePayload) {
	f, removed := r.box.delivered(recipient, done.StreamID, done.OK)
	if f == nil {
		return
	}
	if !removed {
		r.logf("stored file %q to #%d failed (%s); kept for a later retry", f.meta.Filename, recipient, done.Reason)
		return
	}
	sender, ok := r.sm.Get(f.fromID)
	if !ok || sender.Username != f.from {
		return // the sender's session has ended; nobody to tell
	}
	reason := ""
	if !done.OK {
		reason = "the recipient could not receive the stored file: " + done.Reason
	}
	r.sendFileDone(f.fromID, protocol.FileDonePayload{StreamID: f.fromStream, OK: done.OK, Reason: reason})
}

// DeliverStored sends a newly signed-in user everything that waited for them.
func (r *Router) DeliverStored(sess *ClientSession) {
	for _, body := range r.box.takeReceipts(sess.Username) {
		r.enqueueChatStream(protocol.ServerID, sess.UserID, protocol.MsgReceipt, protocol.FlagEOM, sess.UserID, body)
	}
	for _, body := range r.box.takeChats(sess.Username) {
		r.enqueueChatStream(protocol.ServerID, sess.UserID, protocol.MsgChat, protocol.FlagEOM, sess.UserID, body)
	}
	if r.box.pendingFiles(sess.Username) {
		r.kickDelivery(sess.UserID)
	}
}

// kickDelivery wakes the stored-file forwarder of an online recipient.
func (r *Router) kickDelivery(userID uint16) {
	r.mu.Lock()
	rc := r.recipients[userID]
	r.mu.Unlock()
	if rc == nil {
		return
	}
	select {
	case rc.kick <- struct{}{}:
	default:
	}
}

// SweepMailbox aborts idle offline uploads and expires old stored items; the server calls it periodically.
func (r *Router) SweepMailbox(now time.Time) {
	for _, fk := range r.box.sweep(now) {
		r.logf("offline upload #%d from #%d timed out", fk.stream, fk.sender)
	}
}

// runStoredForwarder sends stored files to one recipient, one after another, streaming them from disk.
func (r *Router) runStoredForwarder(rc *recipient) {
	defer r.wg.Done()
	stream := protocol.FirstFileStream
	for {
		select {
		case <-rc.done:
			return
		case <-rc.kick:
		}
		for {
			f := r.box.claimFile(rc.sess.Username, rc.sess.UserID, stream)
			if f == nil {
				break
			}
			cur := stream
			stream++
			if stream < protocol.FirstFileStream {
				stream = protocol.FirstFileStream
			}
			if !r.sendStoredFile(rc, cur, f) {
				r.box.releaseFile(rc.sess.UserID, cur)
				return
			}
		}
	}
}

// sendStoredFile streams one stored file to its recipient; it reports false if the recipient went away.
func (r *Router) sendStoredFile(rc *recipient, stream uint16, f *storedFile) bool {
	userID := rc.sess.UserID
	file, err := os.Open(f.path)
	if err != nil {
		r.logf("stored file %q is unreadable: %v", f.meta.Filename, err)
		r.box.discard(f)
		return true
	}
	defer file.Close()
	send := func(msgType uint8, seq uint32, flags uint8, body []byte) bool {
		select {
		case <-rc.done:
			return false
		default:
		}
		addr, key, ok := r.sm.Endpoint(userID)
		if !ok {
			return false
		}
		hdr := protocol.UDPHeader{MsgType: msgType, Flags: flags, StreamID: stream, SenderID: protocol.ServerID, TargetID: userID, SeqNum: seq}
		return r.ep.SendReliable(userID, addr, key, hdr, body) == nil
	}
	initBody, err := protocol.MarshalJSONCompact(f.meta)
	if err != nil || !send(protocol.MsgFileInit, 0, 0, initBody) {
		return false
	}
	remaining := f.meta.Size
	for seq := uint32(1); seq <= f.meta.TotalChunks; seq++ {
		n := int64(protocol.MaxChunkSize)
		if remaining < n {
			n = remaining
		}
		chunk := make([]byte, n)
		if _, err := io.ReadFull(file, chunk); err != nil {
			r.logf("reading stored file %q: %v", f.meta.Filename, err)
			r.box.discard(f)
			return true
		}
		remaining -= n
		if !send(protocol.MsgFileChunk, seq, 0, chunk) {
			return false
		}
	}
	return send(protocol.MsgFileEnd, f.meta.TotalChunks+1, protocol.FlagEOM, nil)
}

// prepareSpoolDir creates dir (or a private temporary one) and removes leftovers from an earlier run.
func prepareSpoolDir(dir string) (string, bool, error) {
	if dir == "" {
		d, err := os.MkdirTemp("", "lancollab-spool-")
		return d, true, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", false, err
	}
	for _, pattern := range []string{"lc-*.part", "lc-*.bin"} {
		old, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, p := range old {
			_ = os.Remove(p)
		}
	}
	return dir, false, nil
}