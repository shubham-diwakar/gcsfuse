package filecache

import (
	"context"

	"github.com/jacobsa/syncutil"
)

type FileDownloadJob struct {
	FileInfo
	fileSize int
	status   string
	ctx      context.Context
	cancel   func()
	mu       syncutil.InvariantMutex
}

func (fdj *FileDownloadJob) cancelJob() {
	if fdj.status == "RUNNING" {
		fdj.cancel()

		// Check if we need to write status to fdm.channels or will it be done implicitly.
	}
}

func (fdj *FileDownloadJob) downloadFile(statusUpdate chan *FileDownloadJob) {
	// Download the file using fdj.ctx as context
	// Notify listeners as required.
	// send success or failure like below.
	fdj.status = "SUCCESS"
	statusUpdate <- fdj
}

func (fdj *FileDownloadJob) RegisterForDownloadProgress() {

}

func (fdj *FileDownloadJob) Deregister() {

}
