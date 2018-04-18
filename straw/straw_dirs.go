package straw

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/uw-labs/straw"
)

func seqToPath(basepath string, seq int) string {
	seqStr := fmt.Sprintf("%014d", seq)
	path := basepath
	for len(seqStr) > 0 {
		path = filepath.Join(path, seqStr[0:2])
		seqStr = seqStr[2:]
	}
	return path
}

func nextSequence(ss straw.StreamStore, basedir string) (int, error) {
	_, err := ss.Stat(seqToPath(basedir, 0))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return -1, err
	}

	dir := basedir
	total := 0
	for i := 0; i < 7; i++ {
		fis, err := ss.Readdir(dir)
		if err != nil {
			return -1, err
		}
		fi := fis[len(fis)-1]
		if i < 6 && !fi.IsDir() {
			return -1, err
		}
		dir = filepath.Join(dir, fi.Name())
		num, err := strconv.Atoi(fi.Name())
		if err != nil {
			return -1, err
		}
		total *= 100
		total += num
	}
	return total + 1, nil
}
