package downloader

import (
	"strings"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/locker"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
)

type FileDownloadManager struct {
	fileInfoCache    *lru.Cache
	fileDownloadJobs map[string]*FileDownloadJob
	mu               locker.Locker
}

func NewFileDownloadManager(fileInfoCache *lru.Cache) (fdm FileDownloadManager) {
	fdm = FileDownloadManager{fileInfoCache: fileInfoCache}
	fdm.mu = locker.New("FDM", func() {})
	fdm.fileDownloadJobs = make(map[string]*FileDownloadJob)
	return
}
func getObjectPath(bucketName string, objectName string) (objectPath string) {
	return strings.Join([]string{bucketName, objectName}, "/")
}
func (fdm *FileDownloadManager) GetDownloadJob(object *gcs.MinObject, bucket gcs.Bucket, downloadPath string) (fdj *FileDownloadJob) {
	fdm.mu.Lock()
	defer fdm.mu.Unlock()
	objectPath := getObjectPath(bucket.Name(), object.Name)
	fileDownloadJob, ok := fdm.fileDownloadJobs[objectPath]
	if !ok {
		fileDownloadJob = NewFileDownloadJob(object, bucket, downloadPath, fdm.fileInfoCache)
		fdm.fileDownloadJobs[objectPath] = fileDownloadJob
	}
	return fileDownloadJob
}

func (fdm *FileDownloadManager) CancelJob(objectName string, bucketName string) (err error) {
	fdm.mu.Lock()
	defer fdm.mu.Unlock()
	objectPath := getObjectPath(bucketName, objectName)
	fileDownloadJob, ok := fdm.fileDownloadJobs[objectPath]
	if ok {
		err = fileDownloadJob.Cancel()
		delete(fdm.fileDownloadJobs, objectPath)
		return
	}
	return
}
