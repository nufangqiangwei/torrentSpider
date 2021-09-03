package krpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"testTorrent/torrent/bencode"
)

// https://torrent/issues/166
func TestUnmarshalBadError(t *testing.T) {
	var e Error
	err := bencode.Unmarshal([]byte(`l5:helloe`), &e)
	require.Error(t, err)
}
