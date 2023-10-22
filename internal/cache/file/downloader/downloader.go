package downloader

import (
	"os"
	"strings"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/locker"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
)

type FileDownloadManager struct {
	fileInfoCache *lru.Cache
	uid           uint32
	gid           uint32
	perm          os.FileMode
	jobs          map[string]*Job
	mu            locker.Locker
}

func NewFileDownloadManager(fileInfoCache *lru.Cache) (fdm FileDownloadManager) {
	fdm = FileDownloadManager{fileInfoCache: fileInfoCache}
	fdm.mu = locker.New("FDM", func() {})
	fdm.jobs = make(map[string]*Job)
	return
}
func getObjectPath(bucketName string, objectName string) (objectPath string) {
	return strings.Join([]string{bucketName, objectName}, "/")
}
func (fdm *FileDownloadManager) GetDownloadJob(object *gcs.MinObject, bucket gcs.Bucket, downloadPath string) (fdj *Job) {
	fdm.mu.Lock()
	defer fdm.mu.Unlock()
	objectPath := getObjectPath(bucket.Name(), object.Name)
	fileDownloadJob, ok := fdm.jobs[objectPath]
	if !ok {
		// To-Do(sethiay): Correct sequentialReadSizeMb
		fileSpec := data.FileSpec{
			Path: downloadPath,
			Perm: fdm.perm,
		}
		fileDownloadJob = NewJob(object, bucket, fdm.fileInfoCache, 200, fileSpec)
		fdm.jobs[objectPath] = fileDownloadJob
	}
	return fileDownloadJob
}

func (fdm *FileDownloadManager) CancelJob(objectName string, bucketName string) (err error) {
	fdm.mu.Lock()
	defer fdm.mu.Unlock()
	objectPath := getObjectPath(bucketName, objectName)
	fileDownloadJob, ok := fdm.jobs[objectPath]
	if ok {
		fileDownloadJob.Cancel()
		delete(fdm.jobs, objectPath)
		return
	}
	return
}
