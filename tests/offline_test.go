package tests

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"lancollab/internal/client"
	"lancollab/internal/server"
)

// signOut logs a user out and waits until another client no longer sees them online.
func signOut(t *testing.T, who, observer *client.Controller) {
	t.Helper()
	name := who.Username()
	if err := who.Logout(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		online := false
		for _, p := range observer.Peers() {
			if p.Name == name {
				online = true
			}
		}
		if !online {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still looks online", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// spoolFiles lists the server's stored or partial offline files.
func spoolFiles(t *testing.T, srv *server.Server) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(srv.SpoolDir(), "lc-*"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestUsersListIncludesOfflineAccounts(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	users := alice.Users()
	if strings.Join(users, ",") != "alice,bob" {
		t.Fatalf("users = %v, want alice and bob (bob offline)", users)
	}
	if err := alice.SendChatTo("nobody", "hi"); err == nil {
		t.Fatal("a message to an unknown user must be refused")
	}
	if _, err := alice.SendFileTo("nobody", "a.txt", []byte("x")); err == nil {
		t.Fatal("a file to an unknown user must be refused")
	}
}

func TestOfflineChatIsDeliveredOnSignIn(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	aliceEvents := subscribe(t, alice)
	for _, text := range []string{"first", "second"} {
		if err := alice.SendChatTo("bob", text); err != nil {
			t.Fatalf("message to offline bob: %v", err)
		}
	}
	waitEvent(t, aliceEvents, 2*time.Second, "queued echo", func(ev client.ClientEvent) bool {
		return ev.Kind == client.EventChat && ev.Self && ev.Queued && ev.Peer == "bob"
	})
	time.Sleep(100 * time.Millisecond) // let the server store both messages

	bob2 := newClient(t, srv, "bob")
	events := subscribe(t, bob2)
	for _, want := range []string{"first", "second"} {
		ev := waitEvent(t, events, 5*time.Second, "stored chat "+want, func(ev client.ClientEvent) bool {
			return ev.Kind == client.EventChat && !ev.Self && ev.Text == want
		})
		if ev.From != "alice" || ev.Peer != "alice" || !ev.Stored || !ev.Direct {
			t.Fatalf("stored message delivered as %+v", ev)
		}
	}
}

func TestOfflineFileIsDeliveredOnSignInAndSenderIsTold(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	data := make([]byte, 300<<10)
	_, _ = rand.Read(data)
	alice.SetDropRate(0.05)
	stream, err := alice.SendFileTo("bob", "photo.png", data)
	if err != nil {
		t.Fatal(err)
	}
	queued := waitTransferStage(t, alice, stream, client.DirectionSend, client.StageQueued, 10*time.Second)
	if queued.PeerName != "bob" {
		t.Fatalf("queued transfer names %q, want bob", queued.PeerName)
	}
	if n := len(spoolFiles(t, srv)); n != 1 {
		t.Fatalf("the server should hold 1 stored file, has %d", n)
	}

	bob2, dir := newClientDir(t, srv, "bob")
	bob2.SetDropRate(0.05)
	var got client.TransferProgress
	deadline := time.Now().Add(15 * time.Second)
	for {
		found := false
		for _, tp := range bob2.Transfers() {
			if tp.Direction == client.DirectionRecv && tp.Stage == client.StageVerified {
				got, found = tp, true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob never received the stored file: %+v", bob2.Transfers())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got.PeerName != "alice" {
		t.Fatalf("stored file comes from %q, want alice", got.PeerName)
	}
	saved, err := os.ReadFile(filepath.Join(dir, "photo.png"))
	if err != nil || !bytes.Equal(saved, data) {
		t.Fatalf("stored file is not byte-exact (err %v)", err)
	}
	// Alice is still online, so she learns that bob has it.
	waitTransferStage(t, alice, stream, client.DirectionSend, client.StageVerified, 5*time.Second)
	deadline = time.Now().Add(3 * time.Second)
	for len(spoolFiles(t, srv)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the delivered file was not removed from the spool: %v", spoolFiles(t, srv))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOfflineFileIsDeliveredAfterSenderLeft(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	data := bytes.Repeat([]byte("lan"), 10000)
	stream, err := alice.SendFileTo("bob", "notes.txt", data)
	if err != nil {
		t.Fatal(err)
	}
	waitTransferStage(t, alice, stream, client.DirectionSend, client.StageQueued, 10*time.Second)
	if err := alice.Logout(); err != nil {
		t.Fatal(err)
	}

	bob2, dir := newClientDir(t, srv, "bob")
	deadline := time.Now().Add(10 * time.Second)
	for {
		saved, err := os.ReadFile(filepath.Join(dir, "notes.txt"))
		if err == nil && bytes.Equal(saved, data) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob never received the file stored by alice, who has left: %+v", bob2.Transfers())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOfflineFileOverMailboxLimitIsRefused(t *testing.T) {
	srv := startServer(t, func(s *server.Server) {
		if err := s.SetMailboxLimit(100 << 10); err != nil {
			t.Fatal(err)
		}
	})
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	stream, err := alice.SendFileTo("bob", "big.bin", make([]byte, 200<<10))
	if err != nil {
		t.Fatal(err)
	}
	tp := waitTransferStage(t, alice, stream, client.DirectionSend, client.StageFailed, 10*time.Second)
	if !strings.Contains(tp.Error, "mailbox is full") {
		t.Fatalf("unexpected failure reason %q", tp.Error)
	}
	if n := len(spoolFiles(t, srv)); n != 0 {
		t.Fatalf("a refused upload left %d files in the spool", n)
	}
}

func TestOfflineUploadIsDiscardedWhenSenderLeaves(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	alice.SetDropRate(0.3) // slow the upload down so it is still running when alice leaves
	stream, err := alice.SendFileTo("bob", "half.bin", make([]byte, 8<<20))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if tp, ok := transferOf(alice, stream, client.DirectionSend); ok && tp.Percent > 0 && len(spoolFiles(t, srv)) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the upload to the server never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = alice.Logout()

	deadline = time.Now().Add(5 * time.Second)
	for len(spoolFiles(t, srv)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the partial upload was not deleted: %v", spoolFiles(t, srv))
		}
		time.Sleep(20 * time.Millisecond)
	}
	bob2 := newClient(t, srv, "bob")
	time.Sleep(300 * time.Millisecond)
	if n := len(bob2.Transfers()); n != 0 {
		t.Fatalf("bob received %d transfers from an upload that never finished", n)
	}
}
