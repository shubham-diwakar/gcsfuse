package downloader

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/locker"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
	"golang.org/x/net/context"
)

type FileDownloadJobStatus string

const (
	NOT_STARTED FileDownloadJobStatus = "not_started"
	RUNNING     FileDownloadJobStatus = "running"
	FINISHED    FileDownloadJobStatus = "finished"
	ERROR       FileDownloadJobStatus = "error"
	CANCELLED   FileDownloadJobStatus = "cancelled"
)
const ChunkSize = 8 * 1024 * 1024

type downloadProgress struct {
	err    error
	offset int64
}
type downloadJobSubscriber struct {
	notificationCh chan<- downloadProgress
	offset         int64
}
type FileDownloadJob struct {
	object           *gcs.MinObject
	bucket           gcs.Bucket
	fileDownloadPath string
	status           FileDownloadJobStatus
	offset           int64
	subscribers      list.List
	fileInfoCache    *lru.Cache
	mu               locker.Locker
	cancelC          chan bool
	err              error
}

func NewFileDownloadJob(object *gcs.MinObject, bucket gcs.Bucket, fileDownloadPath string, fileInfoCache *lru.Cache) (fdj *FileDownloadJob) {
	fdj = &FileDownloadJob{
		object:           object,
		bucket:           bucket,
		fileDownloadPath: fileDownloadPath,
		status:           NOT_STARTED,
		offset:           0,
		fileInfoCache:    fileInfoCache,
		cancelC:          make(chan bool, 1),
		err:              nil,
	}
	fdj.mu = locker.New("FDJ", func() {})
	return
}
func (fdj *FileDownloadJob) Cancel() (err error) {
	fdj.mu.Lock()
	if fdj.status != RUNNING {
		fdj.mu.Unlock()
		err = fmt.Errorf(fmt.Sprintf("current status of file downloader is not running %s", fdj.status))
		return err
	}
	fdj.cancelC <- true
	close(fdj.cancelC)
	fdj.mu.Unlock()
	return nil
}

func (fdj *FileDownloadJob) addSubscriber(offset int64) (notificationCh <-chan downloadProgress) {
	subC := make(chan downloadProgress, 1)
	fdj.subscribers.PushBack(downloadJobSubscriber{subC, offset})
	return subC
}

func (fdj *FileDownloadJob) notifySubscribers(downloadErr error) {
	currOffset := fdj.offset
	if downloadErr != nil {
		currOffset = -1
	}
	currDownloadProgress := downloadProgress{downloadErr, currOffset}
	subItr := fdj.subscribers.Front()
	for subItr != nil {
		subItrValue := subItr.Value.(downloadJobSubscriber)
		nextSubItr := subItr.Next()
		if currOffset == -1 || currOffset >= subItrValue.offset {
			subItrValue.notificationCh <- currDownloadProgress
			close(subItrValue.notificationCh)
			fdj.subscribers.Remove(subItr)
		}
		subItr = nextSubItr
	}
}

func (fdj *FileDownloadJob) throwErrorWhileDownloading(errToThrow error) {
	fdj.mu.Lock()
	fdj.notifySubscribers(errToThrow)
	fdj.status = ERROR
	fdj.err = errToThrow
	fdj.mu.Unlock()
}

// Parse the time string into a time value.
var FixBucketCreationTime string = "2023-03-08T12:00:00Z"

func (fdj *FileDownloadJob) updateFileInfoCache() (err error) {
	tr, _ := time.Parse(time.RFC3339, FixBucketCreationTime)
	fileInfoKey := data.FileInfoKey{
		BucketName: fdj.bucket.Name(),
		//change when bucket creation time is exposed.
		BucketCreationTime: tr,
		ObjectName:         fdj.object.Name,
	}
	fileInfoKeyName, err := fileInfoKey.Key()
	if err != nil {
		err = fmt.Errorf("error while creating fileInfoKey %v", fileInfoKeyName)
		return
	}
	// Should AsyncJob access be really counted as Look up ? I think it should
	// be an access not lookup
	existingFileInfo := fdj.fileInfoCache.LookUp(fileInfoKeyName)
	fileInfo := data.FileInfo{Key: fileInfoKey,
		ObjectGeneration: strconv.FormatInt(fdj.object.Generation, 10),
		FileSize:         fdj.object.Size, Offset: uint64(fdj.offset)}
	// We need to make object generation in FileInfo as int64.
	if existingFileInfo != nil && existingFileInfo.(data.FileInfo).ObjectGeneration != fileInfo.ObjectGeneration {
		err = fmt.Errorf(fmt.Sprintf("generation of object being downloaded: %s and that of present in fileInfo cache: %s are not same",
			fileInfo.ObjectGeneration, existingFileInfo.(data.FileInfo).ObjectGeneration))
		return
	}
	_, err = fdj.fileInfoCache.Insert(fileInfoKeyName, fileInfo)
	if err != nil {
		return
	}
	return
}

func (fdj *FileDownloadJob) downloadObject() {
	err := os.MkdirAll(filepath.Dir(fdj.fileDownloadPath), 0777)
	if err != nil {
		err = fmt.Errorf("downloadObject: Error in creating file directories %v", err)
		fdj.throwErrorWhileDownloading(err)
		return
	}
	flag := os.O_RDWR
	_, err = os.Stat(fdj.fileDownloadPath)
	if err != nil {
		flag = flag | os.O_CREATE
	}
	file, err := os.OpenFile(fdj.fileDownloadPath, flag, 0777)
	if err != nil {
		err = fmt.Errorf("downloadObject: Error in creating file %v", err)
		fdj.throwErrorWhileDownloading(err)
		return
	}
	ctx, _ := context.WithCancel(context.Background())
	rc, err := fdj.bucket.NewReader(
		ctx,
		&gcs.ReadObjectRequest{
			Name:       fdj.object.Name,
			Generation: fdj.object.Generation,
			Range: &gcs.ByteRange{
				Start: uint64(0),
				Limit: fdj.object.Size,
			},
			ReadCompressed: fdj.object.HasContentEncodingGzip(),
		})
	if err != nil {
		err = fmt.Errorf("NewReader: %w", err)
		fdj.throwErrorWhileDownloading(err)
		return
	}

	var start int64
	end := int64(fdj.object.Size)

	for {
		select {
		case <-fdj.cancelC:
			fdj.mu.Lock()
			fdj.status = CANCELLED
			err := fmt.Errorf("downloadObject: The download is cancelled")
			fdj.notifySubscribers(err)
			fdj.mu.Unlock()
			return
		default:
			if start < end {
				maxRead := min(end-start, ChunkSize)
				_, err = file.Seek(int64(start), 0)
				if err != nil {
					return
				}
				_, err := io.CopyN(file, rc, maxRead)
				if err != nil && err != io.EOF {
					err = fmt.Errorf("at the time of reading %v", err)
					fdj.throwErrorWhileDownloading(err)
					return
				}
				start = maxRead + start

				fdj.mu.Lock()
				fdj.offset += maxRead
				err = fdj.updateFileInfoCache()
				fdj.notifySubscribers(err)
				if err != nil {
					fdj.status = ERROR
					fdj.mu.Unlock()
					return
				}
				fdj.mu.Unlock()
			} else {
				fdj.mu.Lock()
				fdj.status = FINISHED
				fdj.mu.Unlock()
				return
			}
		}
	}
}

func (fdj *FileDownloadJob) Download(offset uint64, waitForDownload bool) (err error) {
	fdj.mu.Lock()
	if fdj.status == FINISHED {
		defer fdj.mu.Unlock()
		if fdj.offset < int64(offset) {
			return fmt.Errorf("download: The job status is finished but the requested offset is greater than offset of download job")
		}
		return nil
	} else if fdj.status == NOT_STARTED {
		go fdj.downloadObject()
		fdj.status = RUNNING
	} else if fdj.status == ERROR {
		err = fmt.Errorf("download job is currently in error state %v", fdj.err)
		fdj.mu.Unlock()
		return
	} else if fdj.status == CANCELLED {
		err = fmt.Errorf("download job is currently in cancelled state")
		fdj.mu.Unlock()
		return
	}
	if !waitForDownload {
		fdj.mu.Unlock()
		return nil
	}
	notificationC := fdj.addSubscriber(int64(offset))
	fdj.mu.Unlock()
	currDownloadProgress := <-notificationC
	return currDownloadProgress.err
}
