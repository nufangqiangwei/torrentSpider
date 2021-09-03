package torrent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"testTorrent/torrent/internal/testutil"
)

func TestReaderReadContext(t *testing.T) {
	cl, err := NewClient(TestingConfig(t))
	require.NoError(t, err)
	defer cl.Close()
	tt, err := cl.AddTorrent(testutil.GreetingMetaInfo())
	require.NoError(t, err)
	defer tt.Drop()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()
	r := tt.Files()[0].NewReader()
	defer r.Close()
	_, err = r.ReadContext(ctx, make([]byte, 1))
	require.EqualValues(t, context.DeadlineExceeded, err)
}
