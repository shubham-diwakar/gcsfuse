package filecache

import (
	"github.com/googlecloudplatform/gcsfuse/internal/locker"
	"golang.org/x/net/context"
)

type FileDownloadManager struct {
	// This should be a queue.
	jobDetails  map[string]FileDownloadJob
	jobChannels chan *FileDownloadJob
	mu          locker.Locker
}

const maxGoRoutinesForDownload = 10

var currentGoRoutines = 0

// TODOs:
// 1. Cancelling a job needs to be handled.
// 2. Add max retries for retyring a failed download
// 3. MaxGoroutines should be based on tcp connections flag
// 4. We can choose to wait for all files to be downloaded when unmount is called.
// Shouldn't be required for v1 since persisting data between mounts is not handled in v1.
// For unmount event, create a pub-sub mechanism on unmount where different parts of the code can register.
func InitFileDownloadManager() FileDownloadManager {
	jobChannels := make(chan *FileDownloadJob, maxGoRoutinesForDownload)
	fdm := FileDownloadManager{
		jobDetails:  make(map[string]FileDownloadJob),
		jobChannels: jobChannels,
	}

	go func() {
		for fileDi := range jobChannels {
			fdm.mu.Lock()
			switch fileDi.status {
			case "COMPLETED":
				currentGoRoutines = currentGoRoutines - 1
				// POP from jobDetails and start download
			case "FAILED":
				currentGoRoutines = currentGoRoutines - 1
				// push the failed one to jobDetails and pop new one and start download.
			default:
				// Something weird happened. log it.

			}
			fdm.mu.Unlock()
		}
	}()

	return fdm
}

func (fdm *FileDownloadManager) DownloadFile(ctx context.Context, fileInfo FileInfo, fileSize int) (fdj FileDownloadJob) {
	fdm.mu.Lock()
	defer fdm.mu.Unlock()
	fdj, ok := fdm.jobDetails[fileInfo.name]
	// If it is already queued for download, ignore the call. Right now we will not
	// work on prioritizing the job just because it is called multiple times.
	if ok {
		return
	}

	fdj = FileDownloadJob{
		FileInfo: FileInfo{
			name: fileInfo.name,
		},
		status:   "NOT_STARTED",
		fileSize: fileSize,
	}

	fdm.jobDetails[fileInfo.name] = fdj
	ctx, cancel := context.WithCancel(ctx)
	fdj.ctx = ctx
	fdj.cancel = cancel

	if currentGoRoutines < maxGoRoutinesForDownload {
		go fdj.downloadFile(fdm.jobChannels)
		currentGoRoutines++
	}

	return
}
