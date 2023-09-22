package filecache

import (
	"os"

	"golang.org/x/net/context"
)

type FileInfo struct {
	name   string
	offset int64
}
type FileCacheHandler struct {
	fileDetails         map[string]FileInfo
	fileDownloadManager FileDownloadManager
}

func InitFileCacheHandler() FileCacheHandler {
	fdm := InitFileDownloadManager()
	fch := FileCacheHandler{
		fileDetails:         make(map[string]FileInfo),
		fileDownloadManager: fdm,
	}

	// load the fileDetails asynchronously from disk post v1.
	return fch
}

func (fch *FileCacheHandler) ReadFile(ctx context.Context, fileName string, fileSize int64, triggerDownload bool) *CacheFileHandle {
	// check if locking is required.
	var info, ok = fch.fileDetails[fileName]
	var fh *os.File
	var fdj FileDownloadJob

	if ok && info.offset == fileSize {
		fh, _ = os.Open(fileName)
	} else if triggerDownload {
		// Creating the file incase it is not in cache and triggerDownload is true.
		fh, _ = os.OpenFile(fileName, os.O_CREATE, 0)

		if !ok {
			info = FileInfo{
				name: fileName,
			}
		}
		// How to get the fileSize, should we pass it or make startDownload do a gcs call.
		// Assuming stat-cache is enabled, it will be a no-op, otherwise its a call to gcs.
		// So we should pass it from here.
		fdj = fch.fileDownloadManager.DownloadFile(ctx, info, int(fileSize))
		// Register for events to update offset.
		// This file will be responsible for updating to disk post v1. Hence it is important to get
		// updates to this file, to be able to update in rocksdb.
	}

	return &CacheFileHandle{
		readFileHandle:  fh,
		fileDownloadJob: fdj,
	}
}
