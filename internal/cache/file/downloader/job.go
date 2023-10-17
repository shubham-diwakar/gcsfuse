package downloader

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/util"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
	"golang.org/x/net/context"
)

// To-Do(sethiay) Need to use creation time of bucket.
// Parse the time string into a time value.
var FixBucketCreationTime string = "2023-03-08T12:00:00Z"

type jobStatusName string

const (
	NOT_STARTED jobStatusName = "NOT_STARTED"
	DOWNLOADING jobStatusName = "DOWNLOADING"
	COMPLETED   jobStatusName = "COMPLETED"
	FAILED      jobStatusName = "FAILED"
	CANCELLED   jobStatusName = "CANCELLED"
)

const ReadChunkSize = 8 * 1024 * 1024

// To-Do(sethiay) Fine tune this timeout with experiments
const TimeoutInSec = time.Duration(time.Second * 5)

// Job downloads the requested object from GCS into the specified local path.
type Job struct {
	/////////////////////////
	// Constant data
	/////////////////////////

	object               *gcs.MinObject
	bucket               gcs.Bucket
	filePath             string
	fileInfoCache        *lru.Cache
	sequentialReadSizeMb int32
	filePerm             os.FileMode
	fileUid              uint32
	fileGid              uint32

	/////////////////////////
	// Mutable state
	/////////////////////////

	status      JobStatus
	subscribers list.List
	cancelCtx   context.Context
	cancelFunc  context.CancelFunc

	mu sync.Mutex
}

// JobStatus represents the status of job.
type JobStatus struct {
	Name   jobStatusName
	Err    error
	Offset int64
}

// jobSubscriber represents a subscriber waiting on async download of job to
// complete downloading at least till the subscribed offset.
type jobSubscriber struct {
	notificationC    chan<- JobStatus
	subscribedOffset int64
}

func NewJob(object *gcs.MinObject, bucket gcs.Bucket, filePath string,
	fileInfoCache *lru.Cache, sequentialReadSizeMb int32, filePerm os.FileMode,
	fileUid uint32, fileGid uint32) (job *Job) {
	job = &Job{
		object:               object,
		bucket:               bucket,
		filePath:             filePath,
		fileInfoCache:        fileInfoCache,
		sequentialReadSizeMb: sequentialReadSizeMb,
		filePerm:             filePerm,
		fileUid:              fileUid,
		fileGid:              fileGid,
	}
	job.init()
	return
}

// init initializes the mutable members of Job corresponding to not started
// state.
func (job *Job) init() {
	job.status = JobStatus{NOT_STARTED, nil, 0}
	job.subscribers = list.List{}
	job.cancelCtx, job.cancelFunc = context.WithCancel(context.Background())
}

// cancel changes the state of job to cancelled and cancels the async download
// job if there. Also, notifies the subscribers of job if any.
//
// Not concurrency safe and requires LOCK(job.mu)
func (job *Job) cancel() {
	if job.status.Name == DOWNLOADING {
		job.cancelFunc()
	}
	job.status.Name = CANCELLED
	job.notifySubscribers()
}

// Cancel calls job.cancel() to change the state of job to cancelled.
//
// Acquires and releases LOCK(job.mu)
func (job *Job) Cancel() {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.cancel()
}

// Reset cancels the job and initialize it to not started status.
//
// Note: Reset doesn't delete the cache file.
// Acquires and releases LOCK(job.mu)
func (job *Job) Reset() {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.cancel()
	job.init()
}

// addSubscriber adds subscriber for download job and returns channel which is
// notified when the download is completed at least till the subscribed offset.
//
// Not concurrency safe and requires LOCK(job.mu)
func (job *Job) addSubscriber(subscribedOffset int64) (notificationC <-chan JobStatus) {
	subscriberC := make(chan JobStatus, 1)
	job.subscribers.PushBack(jobSubscriber{subscriberC, subscribedOffset})
	return subscriberC
}

// notifySubscribers notifies all the subscribers of download job in case of
// error/cancellation or when download is completed till the subscribed offset.
//
// Not concurrency safe and requires LOCK(job.mu)
func (job *Job) notifySubscribers() {
	subItr := job.subscribers.Front()
	for subItr != nil {
		subItrValue := subItr.Value.(jobSubscriber)
		nextSubItr := subItr.Next()
		if job.status.Err != nil || job.status.Name == CANCELLED || job.status.Offset >= subItrValue.subscribedOffset {
			subItrValue.notificationC <- job.status
			close(subItrValue.notificationC)
			job.subscribers.Remove(subItr)
		}
		subItr = nextSubItr
	}
}

// errWhileDownloading changes the status of job to failed and notifies
// subscribers about the download error.
//
// Acquires and releases LOCK(job.mu)
func (job *Job) errWhileDownloading(downloadErr error) {
	job.mu.Lock()
	job.status.Err = downloadErr
	job.status.Name = FAILED
	job.notifySubscribers()
	job.mu.Unlock()
}

// updateFileInfoCache updates the file info cache with latest offset downloaded
// by job. Returns error in case of failure and when generation in file info
// doesn't match generation of object in job.
//
// Not concurrency safe and requires LOCK(job.mu)
func (job *Job) updateFileInfoCache() (err error) {
	// To-Do(sethiay): Remove this to use creation time of bucket.
	tr, _ := time.Parse(time.RFC3339, FixBucketCreationTime)
	fileInfoKey := data.FileInfoKey{
		BucketName: job.bucket.Name(),
		//change when bucket creation time is exposed.
		BucketCreationTime: tr,
		ObjectName:         job.object.Name,
	}
	fileInfoKeyName, err := fileInfoKey.Key()
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error while calling FileInfoKey.Key() for %s %v", fileInfoKeyName, err))
		return
	}

	// To-Do(sethiay/raj-prince): Should AsyncJob access be really counted as Look up ? I think it should
	// be an access not lookup because this access is not the user access and hence should not change LRU order in cache.
	existingFileInfo := job.fileInfoCache.LookUp(fileInfoKeyName)
	updatedFileInfo := data.FileInfo{Key: fileInfoKey,
		// To-Do (sethiay) : We need to make object generation & Offset in FileInfo as int64.
		ObjectGeneration: strconv.FormatInt(job.object.Generation, 10),
		FileSize:         job.object.Size, Offset: uint64(job.status.Offset)}

	// generation of object in job should be equal to that of file in cache.
	if existingFileInfo != nil && existingFileInfo.(data.FileInfo).ObjectGeneration != updatedFileInfo.ObjectGeneration {
		err = fmt.Errorf(fmt.Sprintf("generation of object being downloaded %s and that of present in fileInfo cache %s are not equal",
			updatedFileInfo.ObjectGeneration, existingFileInfo.(data.FileInfo).ObjectGeneration))
		return
	}

	_, err = job.fileInfoCache.Insert(fileInfoKeyName, updatedFileInfo)
	if err != nil {
		err = fmt.Errorf(fmt.Sprintf("error while inserting updatedFileInfo to the FileInfoCache %s: %v", updatedFileInfo.Key, err))
		return
	}

	return
}

// downloadObjectAsync downloads the backing GCS object into a file as part of
// file cache using NewReader method of gcs.Bucket.
//
// Note: There can only be one async download running for a job at a time.
// Acquires and releases LOCK(job.mu)
func (job *Job) downloadObjectAsync(ctx context.Context) {
	file, err := util.CreateFile(job.filePath, job.filePerm, os.O_RDWR, job.fileUid, job.fileGid)
	if err != nil {
		err = fmt.Errorf("downloadObjectAsync: error in creating cache file: %v", err)
		job.errWhileDownloading(err)
		return
	}

	// Create new reader
	var newReader io.ReadCloser

	var start, end int64
	end = int64(job.object.Size)
	newReaderLimit := min(start+int64(job.sequentialReadSizeMb), end)

	for {
		select {
		case <-job.cancelCtx.Done():
			job.mu.Lock()
			job.status.Name = CANCELLED
			job.cancel()
			job.mu.Unlock()
			return
		default:
			if start < end {
				if newReader == nil {
					newReader, err = job.bucket.NewReader(
						ctx,
						&gcs.ReadObjectRequest{
							Name:       job.object.Name,
							Generation: job.object.Generation,
							Range: &gcs.ByteRange{
								Start: uint64(start),
								Limit: uint64(newReaderLimit),
							},
							ReadCompressed: job.object.HasContentEncodingGzip(),
						})
					if err != nil {
						err = fmt.Errorf(fmt.Sprintf("downloadObjectAsync: error in creating NewReader with start %d and limit %d: %v", start, newReaderLimit, err))
						job.errWhileDownloading(err)
						return
					}
				}

				maxRead := min(end-start, ReadChunkSize)
				_, err = file.Seek(start, 0)
				if err != nil {
					err = fmt.Errorf(fmt.Sprintf("downloadObjectAsync: error while seeking file handle, seek %d: %v", start, err))
					job.errWhileDownloading(err)
					return
				}

				// copy the contents from new reader to cache file.
				_, readErr := io.CopyN(file, newReader, maxRead)
				if readErr != nil && readErr != io.EOF {
					err = fmt.Errorf("downloadObjectAsync: error at the time of copying content to cache file %v", readErr)
					job.errWhileDownloading(err)
					return
				}

				start += maxRead
				if readErr == io.EOF {
					newReader = nil
					newReaderLimit = min(start+int64(job.sequentialReadSizeMb), end)
				}

				job.mu.Lock()
				job.status.Offset += maxRead
				err = job.updateFileInfoCache()
				// Notify subscribers if file cache is updated.
				if err == nil {
					job.notifySubscribers()
				}
				job.mu.Unlock()
				// change statu of job in case of error while updating file cache.
				if err != nil {
					job.errWhileDownloading(err)
					return
				}
			} else {
				job.mu.Lock()
				job.status.Name = COMPLETED
				job.notifySubscribers()
				job.mu.Unlock()
				return
			}
		}
	}
}

// Download starts async download if not already started and waits till the
// download is completed for given offset if waitForDownload is true.
//
// Acquires and releases LOCK(job.mu)
func (job *Job) Download(ctx context.Context, offset int64, waitForDownload bool) (jobStatus JobStatus) {
	job.mu.Lock()
	var err error
	if int64(job.object.Size) < offset {
		defer job.mu.Unlock()
		jobStatus.Err = fmt.Errorf(fmt.Sprintf("Download: the requested offset %d is greater than the size of object %d", job.object.Size, offset))
		return
	}

	if job.status.Name == COMPLETED {
		defer job.mu.Unlock()
		if job.status.Offset < int64(job.object.Size) {
			err = fmt.Errorf(fmt.Sprintf("Download: the job status is completed but the Offset %d is less than the size of object %d", offset, job.status.Offset))
			jobStatus = job.status
			jobStatus.Err = err
			return jobStatus
		}
		return job.status
	} else if job.status.Name == NOT_STARTED {
		// start the async download
		job.status.Name = DOWNLOADING
		go job.downloadObjectAsync(job.cancelCtx)
	} else if job.status.Name == FAILED || job.status.Name == CANCELLED || !waitForDownload || job.status.Offset >= offset {
		defer job.mu.Unlock()
		jobStatus = job.status
		return
	}

	// Add a subscriber with an offset.
	notificationC := job.addSubscriber(offset)
	// lock is not required when the subscriber is waiting for async download job.
	job.mu.Unlock()

	// Wait till subscriber is notified by async job or the async job is cancelled
	// or till the timeout.
	ctx, _ = context.WithTimeout(ctx, TimeoutInSec)
	select {
	case <-ctx.Done():
		err = fmt.Errorf(fmt.Sprintf("Download: %v", ctx.Err()))
		jobStatus = JobStatus{CANCELLED, err, 0}
	case jobStatus, _ = <-notificationC:
		return
	}
	return
}
