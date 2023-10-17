package util

import (
	"fmt"
	"os"
	"path/filepath"
)

// CreateFile creates file at given path with given permissions and ownership
// and returns file handle for that file opened with given flag.
func CreateFile(path string, perm os.FileMode, flag int, uid uint32, gid uint32) (file *os.File, err error) {
	// Create directory structure if not present
	fileDir := filepath.Dir(path)
	err = os.MkdirAll(fileDir, perm)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error in creating directory structure %s: %v", fileDir, err))
		return
	}

	// Create file if not present.
	_, err = os.Stat(path)
	if err != nil {
		flag = flag | os.O_CREATE
	}

	file, err = os.OpenFile(path, flag, perm)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error in creating file %s: %v", path, err))
		return
	}

	err = os.Chown(path, int(uid), int(gid))
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error in changing filePermission of file %s to fileUid %d and fileGid %d: %v",
			path, uid, gid, err))
		return
	}

	return
}
