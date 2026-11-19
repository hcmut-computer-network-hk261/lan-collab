package client

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	"lancollab/internal/protocol"
)

// receiptRank orders statuses so a late "delivered" never overwrites "seen".
func receiptRank(status string) int {
	switch status {
	case protocol.ReceiptSent:
		return 1
	case protocol.ReceiptDelivered:
		return 2
	case protocol.ReceiptSeen:
		return 3
	}
	return 0
}

// upgradeReceipt returns the later of two statuses.
func upgradeReceipt(cur, next string) string {
	if receiptRank(next) > receiptRank(cur) {
		return next
	}
	return cur
}

// newMsgID returns a random 16-hex-digit ID for a direct message or file.
func newMsgID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// sendReceipt tells the server (by name) that ids from "to" were delivered or seen, in batches that fit one packet.
func (c *Controller) sendReceipt(s *session, status, to string, ids []string) {
	for len(ids) > 0 {
		n := min(len(ids), protocol.MaxReceiptIDs)
		body, err := protocol.MarshalJSONCompact(protocol.ReceiptPayload{Status: status, IDs: ids[:n], To: to})
		if err != nil {
			return
		}
		_ = c.sendOnChatStream(s, protocol.MsgReceipt, protocol.ServerID, body)
		ids = ids[n:]
	}
}

// noteReceived records a direct message or file from peer as delivered-but-unseen and sends "delivered".
func (c *Controller) noteReceived(s *session, peer, id string) {
	if peer == "" || id == "" {
		return
	}
	c.mu.Lock()
	c.unseen[peer] = append(c.unseen[peer], id)
	c.mu.Unlock()
	go c.sendReceipt(s, protocol.ReceiptDelivered, peer, []string{id})
}

// MarkSeen tells peer that this user has seen everything they sent so far (the conversation is open).
func (c *Controller) MarkSeen(peer string) error {
	s, err := c.currentSession()
	if err != nil {
		return err
	}
	c.mu.Lock()
	ids := c.unseen[peer]
	delete(c.unseen, peer)
	c.mu.Unlock()
	if len(ids) > 0 {
		c.sendReceipt(s, protocol.ReceiptSeen, peer, ids)
	}
	return nil
}

// onReceipt applies a receipt to sent messages (in history) and sent files, then tells subscribers.
func (c *Controller) onReceipt(plaintext []byte) {
	var rc protocol.ReceiptPayload
	if err := json.Unmarshal(plaintext, &rc); err != nil || receiptRank(rc.Status) == 0 || len(rc.IDs) == 0 {
		return
	}
	want := make(map[string]bool, len(rc.IDs))
	for _, id := range rc.IDs {
		want[id] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.history {
		ev := &c.history[i]
		if ev.Self && want[ev.ID] {
			ev.Receipt = upgradeReceipt(ev.Receipt, rc.Status)
		}
	}
	for _, tp := range c.transfers {
		if tp.Direction == DirectionSend && want[tp.MsgID] {
			tp.Receipt = upgradeReceipt(tp.Receipt, rc.Status)
		}
	}
	c.emitLocked(ClientEvent{Kind: EventReceipt, Peer: rc.From, Receipt: rc.Status, IDs: rc.IDs})
}
