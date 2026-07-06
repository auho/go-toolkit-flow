package destination

import (
	"context"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

func TestBulk_Receive_WriteCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := &Bulk[storage.MapEntry]{
		writeCtx: ctx,
		// itemsChan is nil — sending to nil channel blocks forever,
		// ensuring the select always picks writeCtx.Done()
	}

	err := b.Receive(storage.MapEntries{{"id": 1}})
	if err == nil {
		t.Fatal("expected error when writeCtx is cancelled, got nil")
	}
}
