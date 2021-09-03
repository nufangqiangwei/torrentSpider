package common

import (
	"testTorrent/torrent/metainfo"
	"testTorrent/torrent/segments"
)

func LengthIterFromUpvertedFiles(fis []metainfo.FileInfo) segments.LengthIter {
	i := 0
	return func() (segments.Length, bool) {
		if i == len(fis) {
			return -1, false
		}
		l := fis[i].Length
		i++
		return l, true
	}
}
