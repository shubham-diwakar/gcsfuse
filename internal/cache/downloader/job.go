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

type downloadJobStatusName string

const (
	NOT_STARTED downloadJobStatusName = "NOT_STARTED"
	DOWNLOADING downloadJobStatusName = "DOWNLOADING"
	COMPLETED   downloadJobStatusName = "COMPLETED"
	FAILED      downloadJobStatusName = "FAILED"
	CANCELLED   downloadJobStatusName = "CANCELLED"
)
const DownloadChunkSize = 8 * 1024 * 1024

type FileDownloadJob struct {
	// Constants
	object           *gcs.MinObject
	bucket           gcs.Bucket
	fileDownloadPath string
	fileInfoCache    *lru.Cache
	perm             os.FileMode
	uid              uint32
	gid              uint32

	status      FileDownloadJobStatus
	subscribers list.List
	cancelCtx   context.Context
	cancelFunc  context.CancelFunc
	mu          locker.Locker
}

type FileDownloadJobStatus struct {
	Name   downloadJobStatusName
	Err    error
	Offset int64
}
type downloadSubscriber struct {
	notificationC    chan<- FileDownloadJobStatus
	subscribedOffset int64
}

func NewFileDownloadJob(object *gcs.MinObject, bucket gcs.Bucket,
	fileDownloadPath string, fileInfoCache *lru.Cache, perm os.FileMode,
	uid uint32, gid uint32) (fdj *FileDownloadJob) {
	fdj = &FileDownloadJob{
		object:           object,
		bucket:           bucket,
		fileDownloadPath: fileDownloadPath,
		fileInfoCache:    fileInfoCache,
		perm:             perm,
		uid:              uid,
		gid:              gid,
	}
	fdj.Init()
	return
}

func (fdj *FileDownloadJob) Init() {
	fdj.status = FileDownloadJobStatus{NOT_STARTED, nil, 0}
	fdj.subscribers = list.List{}
	fdj.cancelCtx, fdj.cancelFunc = context.WithCancel(context.Background())
	fdj.mu = locker.New("FDJ", func() {})
}

func (fdj *FileDownloadJob) Cancel() {
	fdj.mu.Lock()
	defer fdj.mu.Unlock()
	if fdj.status.Name == DOWNLOADING {
		fdj.cancelFunc()
	}
	fdj.status.Name = CANCELLED
	fdj.notifySubscribers()
	return
}

func (fdj *FileDownloadJob) Reset() {
	fdj.Cancel()
	fdj.Init()
}

func (fdj *FileDownloadJob) addSubscriber(subscribedOffset int64) (notificationC <-chan FileDownloadJobStatus) {
	subscriberC := make(chan FileDownloadJobStatus, 1)
	fdj.subscribers.PushBack(downloadSubscriber{subscriberC, subscribedOffset})
	return subscriberC
}

func (fdj *FileDownloadJob) notifySubscribers() {
	subItr := fdj.subscribers.Front()
	for subItr != nil {
		subItrValue := subItr.Value.(downloadSubscriber)
		nextSubItr := subItr.Next()
		if fdj.status.Err != nil || fdj.status.Offset >= subItrValue.subscribedOffset {
			subItrValue.notificationC <- fdj.status
			close(subItrValue.notificationC)
			fdj.subscribers.Remove(subItr)
		}
		subItr = nextSubItr
	}
}

func (fdj *FileDownloadJob) errWhileDownloading(downloadErr error) {
	fdj.mu.Lock()
	fdj.status.Err = downloadErr
	fdj.status.Name = FAILED
	fdj.notifySubscribers()
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
		err = fmt.Errorf(fmt.Sprintf("error while creating FileInfoKey %s %v", fileInfoKeyName, err))
		return
	}
	// Should AsyncJob access be really counted as Look up ? I think it should
	// be an access not lookup
	existingFileInfo := fdj.fileInfoCache.LookUp(fileInfoKeyName)
	updatedFileInfo := data.FileInfo{Key: fileInfoKey,
		// We need to make object generation & Offset in FileInfo as int64.
		ObjectGeneration: strconv.FormatInt(fdj.object.Generation, 10),
		FileSize:         fdj.object.Size, Offset: uint64(fdj.status.Offset)}

	if existingFileInfo != nil && existingFileInfo.(data.FileInfo).ObjectGeneration != updatedFileInfo.ObjectGeneration {
		err = fmt.Errorf(fmt.Sprintf("generation of object being downloaded: %s and that of present in fileInfo cache: %s are not same",
			updatedFileInfo.ObjectGeneration, existingFileInfo.(data.FileInfo).ObjectGeneration))
		return
	}
	_, err = fdj.fileInfoCache.Insert(fileInfoKeyName, updatedFileInfo)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error while inserting updatedFileInfo to the FileInfoCache %s: %v", updatedFileInfo.Key, err))
		return
	}

	return
}

func (fdj *FileDownloadJob) asyncDownloadObject(ctx context.Context) {
	// Create directory structure if not present
	fileDir := filepath.Dir(fdj.fileDownloadPath)
	err := os.MkdirAll(fileDir, fdj.perm)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("asyncDownloadObject: Error in creating directory structure %s: %v", fileDir, err))
		fdj.errWhileDownloading(err)
		return
	}
	// Open file for writing and create if not present.
	flag := os.O_RDWR
	_, err = os.Stat(fdj.fileDownloadPath)
	if err != nil {
		flag = flag | os.O_CREATE
	}
	file, err := os.OpenFile(fdj.fileDownloadPath, flag, fdj.perm)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("downloadObject: Error in creating file %s: %v", fdj.fileDownloadPath, err))
		fdj.errWhileDownloading(err)
		return
	}
	err = os.Chown(fdj.fileDownloadPath, int(fdj.uid), int(fdj.gid))
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("downloadObject: Error in changing permission of cache file %s to uid %s and gid %s: %v",
			fdj.fileDownloadPath, strconv.FormatInt(int64(fdj.uid), 10), strconv.FormatInt(int64(fdj.gid), 10), err))
		fdj.errWhileDownloading(err)
		return
	}
	// Create new reader
	rc, err := fdj.bucket.NewReader(
		ctx,
		&gcs.ReadObjectRequest{
			Name:       fdj.object.Name,
			Generation: fdj.object.Generation,
			Range: &gcs.ByteRange{
				Start: 0,
				Limit: fdj.object.Size,
			},
			ReadCompressed: fdj.object.HasContentEncodingGzip(),
		})

	if err != nil {
		err = fmt.Errorf("asyncDownloadObject: error in creating NewReader: %v", err)
		fdj.errWhileDownloading(err)
		return
	}

	var start int64
	end := int64(fdj.object.Size)

	for {
		select {
		case <-fdj.cancelCtx.Done():
			fdj.Cancel()
			return
		default:
			if start < end {
				maxRead := min(end-start, DownloadChunkSize)
				_, err = file.Seek(start, 0)
				if err != nil {
					err = fmt.Errorf(fmt.Sprintf("error while seeking file handle of asyncDownloadObject, seek %d: %v", start, err))
					fdj.errWhileDownloading(err)
					return
				}

				// copy the contents from new reader to cache file.
				_, err := io.CopyN(file, rc, maxRead)
				if err != nil && err != io.EOF {
					err = fmt.Errorf("error at the time of copying content to cache file %v", err)
					fdj.errWhileDownloading(err)
					return
				}
				start = maxRead + start

				fdj.mu.Lock()
				fdj.status.Offset += maxRead
				err = fdj.updateFileInfoCache()
				if err == nil {
					fdj.notifySubscribers()
				}
				fdj.mu.Unlock()
				if err != nil {
					fdj.errWhileDownloading(err)
				}
			} else {
				fdj.mu.Lock()
				fdj.status.Name = COMPLETED
				fdj.mu.Unlock()
				return
			}
		}
	}
}

func (fdj *FileDownloadJob) Download(ctx context.Context, offset int64, waitForDownload bool) (fileDownloadJobStatus FileDownloadJobStatus) {
	var err error
	fdj.mu.Lock()
	if fdj.status.Name == COMPLETED {
		defer fdj.mu.Unlock()
		if fdj.status.Offset < offset {
			err = fmt.Errorf(fmt.Sprintf("download: The job status is completed but the requested Offset %d is greater than Offset of download job %d", offset, fdj.status.Offset))
			fileDownloadJobStatus = fdj.status
			fileDownloadJobStatus.Err = err
			return fileDownloadJobStatus
		}
		return fdj.status
	} else if fdj.status.Name == NOT_STARTED {
		// start the download
		fdj.status.Name = DOWNLOADING
		go fdj.asyncDownloadObject(fdj.cancelCtx)
	} else if fdj.status.Name == FAILED || fdj.status.Name == CANCELLED || !waitForDownload || fdj.status.Offset >= offset {
		fileDownloadJobStatus = fdj.status
		fdj.mu.Unlock()
		return
	}

	notificationC := fdj.addSubscriber(offset)
	fdj.mu.Unlock()

	select {
	case <-ctx.Done():
		err = fmt.Errorf("context for Download was cancelled")
		fileDownloadJobStatus = FileDownloadJobStatus{CANCELLED, err, -1}
	case fileDownloadJobStatus, _ = <-notificationC:
		return
	}
	return
}
