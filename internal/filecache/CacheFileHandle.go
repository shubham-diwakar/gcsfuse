package filecache

import "os"

type CacheFileHandle struct {
	readFileHandle  *os.File
	fileDownloadJob FileDownloadJob
}

func (cfh *CacheFileHandle) Close() {
	// Cleanup any registered events.
	cfh.fileDownloadJob.cancelJob()
}

func (cfh *CacheFileHandle) Read(offset int64, p []byte, waitForData bool) (n int64, err error) {
	fi, _ := cfh.readFileHandle.Stat()
	if fi.Size() > offset+int64(len(p)) {
		cfh.readFileHandle.Seek(offset, 0)
		cfh.readFileHandle.Read(p)
		return
	}

	if !waitForData {
		return
	}

	// Register for download progress and wait
	// Eg: https://www.linkedin.com/pulse/implementing-pub-sub-golang-sakshyam-ghimire/
	// Ensure that for a file whose download is in progress, multiple reads are happening
	// and multiple readers are waiting, everyone gets the notification.
	return
}
