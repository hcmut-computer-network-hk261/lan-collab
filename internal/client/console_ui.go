package client

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"lancollab/internal/protocol"
)

const consoleHelp = `Commands:
  <text>                 send <text> to everyone
  /msg <user> <text>     send a direct message (<user> is an ID or a name; offline names get it later)
  /send <user> <path>    send a file (an offline user receives it when they sign in)
  /save <file name>      keep a received file (copied to the -downloads folder)
  /peers                 list online users and offline accounts
  /transfers             list file transfers
  /stats                 show link statistics (RTT, cwnd, rwnd, retransmissions)
  /help                  show this help
  /quit                  sign out and exit`

// syncWriter serialises writes from the input loop and the event printer.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) printf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.w, format, args...)
}

// RunConsole signs in and drives the controller from a terminal until /quit (empty credentials are prompted).
func RunConsole(ctrl *Controller, in io.Reader, out io.Writer, username, password string) error {
	w := &syncWriter{w: out}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 64*1024), 64*1024)

	prompt := func(label string) (string, bool) {
		w.printf("%s: ", label)
		if !sc.Scan() {
			return "", false
		}
		return strings.TrimSpace(sc.Text()), true
	}
	var ok bool
	if username == "" {
		if username, ok = prompt("Username"); !ok {
			return io.EOF
		}
	}
	if password == "" {
		if password, ok = prompt("Password"); !ok {
			return io.EOF
		}
	}

	w.printf("Connecting to %s ...\n", ctrl.ServerAddress())
	if err := ctrl.ConnectAndLogin(username, password); err != nil {
		return err
	}
	w.printf("Signed in as %s (#%d). Type /help for commands.\n", ctrl.Username(), ctrl.UserID())

	events := ctrl.SubscribeEvents()
	printerDone := make(chan struct{})
	go func() {
		defer close(printerDone)
		printConsoleEvents(events, w, func(peer string) { go func() { _ = ctrl.MarkSeen(peer) }() })
	}()

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "/") {
			if err := ctrl.SendChat(protocol.BroadcastTarget, line); err != nil {
				w.printf("! %v\n", err)
			}
			continue
		}
		cmd, rest, _ := strings.Cut(line, " ")
		rest = strings.TrimSpace(rest)
		switch cmd {
		case "/help":
			w.printf("%s\n", consoleHelp)
		case "/peers":
			for _, p := range ctrl.Peers() {
				me := ""
				if p.ID == ctrl.UserID() {
					me = " (you)"
				}
				w.printf("  #%-5d %-20s %s:%d%s\n", p.ID, p.Name, p.IP, p.UDPPort, me)
			}
			online := make(map[string]bool)
			for _, p := range ctrl.Peers() {
				online[p.Name] = true
			}
			for _, name := range ctrl.Users() {
				if !online[name] {
					w.printf("  %-6s %-20s offline\n", "-", name)
				}
			}
		case "/msg":
			who, text, _ := strings.Cut(rest, " ")
			if who == "" {
				w.printf("! usage: /msg <user> <text>\n")
				continue
			}
			var err error
			if id, idErr := parseUserID(who); idErr == nil {
				err = ctrl.SendChat(id, text)
			} else {
				err = ctrl.SendChatTo(who, text)
			}
			if err != nil {
				w.printf("! %v\n", err)
			}
		case "/send":
			who, filePath, _ := strings.Cut(rest, " ")
			filePath = strings.Trim(strings.TrimSpace(filePath), `"`)
			if who == "" || filePath == "" {
				w.printf("! usage: /send <user> <path>\n")
				continue
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				w.printf("! cannot read %s: %v\n", filePath, err)
				continue
			}
			var stream uint16
			if id, idErr := parseUserID(who); idErr == nil {
				stream, err = ctrl.SendFile(id, filepath.Base(filePath), data)
			} else {
				stream, err = ctrl.SendFileTo(who, filepath.Base(filePath), data)
			}
			if err != nil {
				w.printf("! %v\n", err)
				continue
			}
			w.printf("  sending %s (%d bytes) to %s on stream %d\n", filepath.Base(filePath), len(data), who, stream)
		case "/save":
			if rest == "" {
				w.printf("! usage: /save <file name>\n")
				continue
			}
			if path, err := ctrl.SaveReceived(strings.Trim(rest, `"`)); err != nil {
				w.printf("! %v\n", err)
			} else {
				w.printf("  saved to %s\n", path)
			}
		case "/transfers":
			for _, tp := range ctrl.Transfers() {
				w.printf("  [%s] %-30s %3d%%  %s  peer #%d  stream %d %s\n", tp.Direction, tp.Filename, tp.Percent, tp.Stage, tp.PeerID, tp.StreamID, tp.Error)
			}
		case "/stats":
			if st, ok := ctrl.Stats(); ok {
				w.printf("  RTT %.2f ms | RTO %d ms | cwnd %d | ssthresh %d | peer rwnd %d | in flight %d | retransmissions %d | sent %d | acked %d\n",
					float64(st.RTTMicros)/1000, st.RTOMillis, st.Cwnd, st.Ssthresh, st.PeerRwnd, st.InFlight, st.Retransmissions, st.PacketsSent, st.PacketsAcked)
			} else {
				w.printf("  not signed in\n")
			}
		case "/quit", "/exit":
			_ = ctrl.Logout()
			ctrl.UnsubscribeEvents(events)
			<-printerDone
			w.printf("Signed out.\n")
			return nil
		default:
			w.printf("! unknown command %s (try /help)\n", cmd)
		}
	}
	_ = ctrl.Logout()
	ctrl.UnsubscribeEvents(events)
	<-printerDone
	return sc.Err()
}

func parseUserID(s string) (uint16, error) {
	n, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSpace(s), "#"), 10, 16)
	if err != nil {
		return 0, err
	}
	return uint16(n), nil
}

// printConsoleEvents prints one line per event; file progress at start, every 25 % and at the end.
// A direct message printed on the console counts as seen; seen may be nil.
func printConsoleEvents(events <-chan ClientEvent, w *syncWriter, seen func(peer string)) {
	lastBucket := make(map[string]int)
	lastStage := make(map[string]string)
	for ev := range events {
		switch ev.Kind {
		case EventChat:
			if ev.Self {
				continue // already typed by the user
			}
			ts := time.UnixMilli(ev.TS).Format("15:04:05")
			if ev.Stored {
				w.printf("[%s] (sent while you were offline) %s: %s\n", ts, ev.From, ev.Text)
			} else if ev.Direct {
				w.printf("[%s] (direct) %s: %s\n", ts, ev.From, ev.Text)
			} else {
				w.printf("[%s] %s: %s\n", ts, ev.From, ev.Text)
			}
			if ev.Direct && seen != nil {
				seen(ev.Peer)
			}
		case EventReceipt:
			if ev.Receipt != "sent" {
				w.printf("* %s by %s\n", ev.Receipt, ev.Peer)
			}
		case EventPeers:
			names := make([]string, 0, len(ev.Peers))
			for _, p := range ev.Peers {
				names = append(names, fmt.Sprintf("%s#%d", p.Name, p.ID))
			}
			w.printf("* online: %s\n", strings.Join(names, ", "))
		case EventFileProgress:
			tp := ev.Progress
			if tp == nil {
				continue
			}
			if tp.Direction == DirectionRecv && tp.Stage == StageReceiving {
				continue // like a messenger, an incoming file appears only once it has arrived
			}
			key := transferKey(tp.Direction, tp.PeerID, tp.StreamID)
			bucket := tp.Percent / 25
			if tp.Stage == lastStage[key] && bucket == lastBucket[key] {
				continue
			}
			lastStage[key], lastBucket[key] = tp.Stage, bucket
			arrow := "->"
			if tp.Direction == DirectionRecv {
				arrow = "<-"
			}
			who := tp.PeerName
			if who == "" {
				who = fmt.Sprintf("#%d", tp.PeerID)
			}
			line := fmt.Sprintf("* file %s %s %s: %d%% (%s)", tp.Filename, arrow, who, tp.Percent, tp.Stage)
			if tp.SavedPath != "" {
				line += fmt.Sprintf(" - type /save %s to keep it", filepath.Base(tp.SavedPath))
			}
			if tp.Error != "" {
				line += " - " + tp.Error
			}
			w.printf("%s\n", line)
		case EventState:
			if ev.State == StateDisconnected && ev.Error != "" {
				w.printf("* disconnected: %s\n", ev.Error)
			}
		case EventError:
			w.printf("! server: %s\n", ev.Error)
		}
	}
}
