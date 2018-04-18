package straw

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/straw"
)

func TestSeqToPath(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("/foo/00/00/00/00/00/00/01", seqToPath("/foo", 1))
	assert.Equal("/foo/99/99/99/99/99/99/99", seqToPath("/foo", 99999999999999))
}

func TestFindLatestEmpty(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	latest, err := nextSequence(ss, "/foo/")
	assert.NoError(err)
	assert.Equal(0, latest)

}

func TestFindLatest(t *testing.T) {
	assert := assert.New(t)

	ss := straw.NewMemStreamStore()

	for i := 0; i < 12345; i++ {
		path := seqToPath("/foo/", i)

		dir := filepath.Dir(path)
		straw.MkdirAll(ss, dir, 0755)

		rc, err := ss.CreateWriteCloser(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rc.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
	}

	latest, err := nextSequence(ss, "/foo/")
	assert.NoError(err)
	assert.Equal(12345, latest)

}
