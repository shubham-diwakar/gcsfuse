package file

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	downloader2 "github.com/googlecloudplatform/gcsfuse/internal/cache/downloader"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
	"golang.org/x/net/context"
)

type CacheHandler struct {
	fileInfoCache *lru.Cache
	fdm           *downloader2.FileDownloadManager
	cacheLocation string
}

func NewCacheHandler(fileInfoCache *lru.Cache, fdm *downloader2.FileDownloadManager, cacheLocation string) (fch *CacheHandler) {
	fch = &CacheHandler{fileInfoCache: fileInfoCache, fdm: fdm, cacheLocation: cacheLocation}
	return
}
func (fch *CacheHandler) getFileDownloadPath(objectName string, bucketName string) string {
	return path.Join(fch.cacheLocation, bucketName, objectName)

}
func (fch *CacheHandler) ReadFile(object *gcs.MinObject, bucket gcs.Bucket, startDownload bool) (fileCacheHandle *CacheHandle, err error) {
	tr, _ := time.Parse(time.RFC3339, downloader2.FixBucketCreationTime)
	fileInfoKey := data.FileInfoKey{ObjectName: object.Name, BucketName: bucket.Name(), BucketCreationTime: tr}
	fileInfoKeyName, err := fileInfoKey.Key()
	if err != nil {
		return
	}
	fileInfo := fch.fileInfoCache.LookUp(fileInfoKeyName)
	if fileInfo == nil {
		fileInfo = data.FileInfo{
			Key:              fileInfoKey,
			ObjectGeneration: strconv.FormatInt(object.Generation, 10),
			Offset:           0,
			FileSize:         object.Size,
		}
	} else {
		fileInfo = fileInfo.(data.FileInfo)
	}

	ctx := context.Background()
	objectStat, err := bucket.StatObject(ctx, &gcs.StatObjectRequest{Name: object.Name, ForceFetchFromGcs: false})
	if err != nil {
		return
	}
	if strconv.FormatInt(objectStat.Generation, 10) != fileInfo.(data.FileInfo).ObjectGeneration {
		err = fmt.Errorf(fmt.Sprintf("Mismatch between generations %s, %s", strconv.FormatInt(objectStat.Generation, 10), fileInfo.(data.FileInfo).ObjectGeneration))
		err = fmt.Errorf(err.Error(), fch.fdm.CancelJob(bucket.Name(), object.Name))
		return
	}

	_, err = fch.fileInfoCache.Insert(fileInfoKeyName, fileInfo)
	if err != nil {
		return
	}
	downloadPath := fch.getFileDownloadPath(object.Name, bucket.Name())
	fileDownloadJob := fch.fdm.GetDownloadJob(object, bucket, downloadPath)
	err = fileDownloadJob.Download(0, false)

	if err != nil {
		return
	}

	// create file handle
	err = os.MkdirAll(filepath.Dir(downloadPath), 0777)
	if err != nil {
		return
	}
	flag := os.O_RDONLY
	_, err = os.Stat(downloadPath)
	if err != nil {
		flag = flag | os.O_CREATE
	}
	fh, err := os.OpenFile(downloadPath, flag, 0777)
	if err != nil {
		return
	}
	return &CacheHandle{fileHandle: fh, fileDownloadJob: fileDownloadJob, fileInfoCache: fch.fileInfoCache}, nil
}
