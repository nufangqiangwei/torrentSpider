package torrent

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/anacrolix/log"
	"github.com/stretchr/testify/require"

	"testTorrent/torrent/internal/testutil"
	"testTorrent/torrent/storage"
)

func TestHashPieceAfterStorageClosed(t *testing.T) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(td)
	tt := &Torrent{
		storageOpener: storage.NewClient(storage.NewFile(td)),
		logger:        log.Default,
	}
	mi := testutil.GreetingMetaInfo()
	info, err := mi.UnmarshalInfo()
	require.NoError(t, err)
	require.NoError(t, tt.setInfo(&info))
	require.NoError(t, tt.storage.Close())
	tt.hashPiece(0)
}
