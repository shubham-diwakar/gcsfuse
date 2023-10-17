package file

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/downloader"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
)

type CacheHandle struct {
	fileHandle      *os.File
	fileDownloadJob *downloader.FileDownloadJob
	fileInfoCache   *lru.Cache
}

func (fch *CacheHandle) Read(object *gcs.MinObject, bucket gcs.Bucket, offset uint64, dst []byte) (n int, err error) {

	if fch.fileHandle == nil {
		err = fmt.Errorf("fileHandle is nil")
		return
	}
	if fch.fileDownloadJob == nil {
		err = fmt.Errorf("fileDownloadJob is nil")
		return
	}

	tr, _ := time.Parse(time.RFC3339, downloader.FixBucketCreationTime)
	fileInfoKey := data.FileInfoKey{ObjectName: object.Name, BucketName: bucket.Name(), BucketCreationTime: tr}
	fileInfoKeyName, err := fileInfoKey.Key()

	if err != nil {
		return
	}
	fch.fileInfoCache.CheckInvariants()

	fileInfo := fch.fileInfoCache.LookUp(fileInfoKeyName)

	if fileInfo == nil {
		err = fmt.Errorf("fileInfo is nil")
		return
	}
	if fileInfo.(data.FileInfo).ObjectGeneration != strconv.FormatInt(object.Generation, 10) {
		err = fmt.Errorf("generation changed")
		return
	}

	if fileInfo.(data.FileInfo).Offset < offset {
		err = fch.fileDownloadJob.Download(offset, true)
		if err != nil {
			return
		}
	}

	_, err = fch.fileHandle.Seek(int64(offset), 0)
	if err != nil {
		return
	}
	n, err = io.ReadFull(fch.fileHandle, dst)
	if err == io.EOF {
		err = nil
	}
	return
}
