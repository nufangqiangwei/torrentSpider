package metainfo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"testTorrent/torrent/bencode"
)

func TestMarshalInfo(t *testing.T) {
	var info Info
	b, err := bencode.Marshal(info)
	assert.NoError(t, err)
	assert.EqualValues(t, "d4:name0:12:piece lengthi0e6:pieces0:e", string(b))
}
