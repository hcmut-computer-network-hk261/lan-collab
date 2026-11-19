package tests

import (
	"testing"
	"time"

	"lancollab/internal/client"
)

// waitReceipt waits for a receipt event with the given status for message id.
func waitReceipt(t *testing.T, events <-chan client.ClientEvent, id, status string) client.ClientEvent {
	t.Helper()
	return waitEvent(t, events, 5*time.Second, status+" receipt", func(ev client.ClientEvent) bool {
		if ev.Kind != client.EventReceipt || ev.Receipt != status {
			return false
		}
		for _, x := range ev.IDs {
			if x == id {
				return true
			}
		}
		return false
	})
}

// sentID returns the ID of this client's own direct message with the given text.
func sentID(t *testing.T, events <-chan client.ClientEvent, text string) string {
	t.Helper()
	ev := waitEvent(t, events, 2*time.Second, "own echo", func(ev client.ClientEvent) bool {
		return ev.Kind == client.EventChat && ev.Self && ev.Text == text
	})
	if ev.ID == "" {
		t.Fatal("a direct message must carry an ID")
	}
	return ev.ID
}

func TestReceiptsSentDeliveredSeenOnline(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	events := subscribe(t, alice)

	if err := alice.SendChatTo("bob", "hello"); err != nil {
		t.Fatal(err)
	}
	id := sentID(t, events, "hello")
	waitReceipt(t, events, id, "sent")
	ev := waitReceipt(t, events, id, "delivered")
	if ev.Peer != "bob" {
		t.Fatalf("receipt names %q, want bob", ev.Peer)
	}
	if err := bob.MarkSeen("alice"); err != nil {
		t.Fatal(err)
	}
	waitReceipt(t, events, id, "seen")
}

func TestReceiptsForOfflineRecipientAndOfflineSender(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)
	events := subscribe(t, alice)

	if err := alice.SendChatTo("bob", "are you there"); err != nil {
		t.Fatal(err)
	}
	id := sentID(t, events, "are you there")
	waitReceipt(t, events, id, "sent") // stored on the server; bob is offline

	bob2 := newClient(t, srv, "bob")
	waitReceipt(t, events, id, "delivered") // bob signed in and got it

	// Alice leaves before bob reads it; the "seen" waits on the server for her.
	signOut(t, alice, bob2)
	if err := bob2.MarkSeen("alice"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := alice.ConnectAndLogin("alice", "pw-alice"); err != nil {
		t.Fatal(err)
	}
	waitReceipt(t, events, id, "seen")
}

func TestReceiptsForOfflineFile(t *testing.T) {
	srv := startServer(t, nil)
	alice := newClient(t, srv, "alice")
	bob := newClient(t, srv, "bob")
	waitPeers(t, alice, 2)
	signOut(t, bob, alice)

	stream, err := alice.SendFileTo("bob", "a.txt", []byte("offline file"))
	if err != nil {
		t.Fatal(err)
	}
	tp := waitTransferStage(t, alice, stream, client.DirectionSend, client.StageQueued, 10*time.Second)
	if tp.Receipt != "sent" || tp.MsgID == "" {
		t.Fatalf("a stored file should be sent with an ID, got %+v", tp)
	}
	bob2 := newClient(t, srv, "bob")
	tp = waitTransferStage(t, alice, stream, client.DirectionSend, client.StageVerified, 10*time.Second)
	if tp.Receipt != "delivered" {
		t.Fatalf("receipt after delivery = %q", tp.Receipt)
	}
	if err := bob2.MarkSeen("alice"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, ok := transferOf(alice, stream, client.DirectionSend); ok && got.Receipt == "seen" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("alice never learned that bob saw the file")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
