// Copyright 2023 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"fmt"
	"os"
	"path"

	"github.com/googlecloudplatform/gcsfuse/internal/cache/data"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/file/downloader"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/lru"
	"github.com/googlecloudplatform/gcsfuse/internal/cache/util"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/gcs"
)

const DefaultFileMode = os.FileMode(0644)

/****** Dummy struct - will be removed ********/

// DownloadJobManager this will be removed once.
//type FileDownloadManager struct {
//}

// CacheHandler Responsible for managing fileInfoCache as well as fileDownloadManager.
type CacheHandler struct {
	fileInfoCache *lru.Cache

	jobManager *downloader.JobManager

	cacheLocation string
}

func (chr *CacheHandler) getLocalFilePath(objectName string, bucketName string) string {
	return path.Join(chr.cacheLocation, bucketName, objectName)

}

func (chr *CacheHandler) createLocalFileReadHandle(objectName string, bucketName string) (*os.File, error) {
	fileSpec := data.FileSpec{
		Path: chr.getLocalFilePath(objectName, bucketName),
		Perm: DefaultFileMode,
	}

	return util.CreateFile(fileSpec, os.O_RDONLY)
}

func NewCacheHandler(fileInfoCache *lru.Cache, jobManager *downloader.JobManager, cacheLocation string) *CacheHandler {
	return &CacheHandler{
		fileInfoCache: fileInfoCache,
		jobManager:    jobManager,
		cacheLocation: cacheLocation,
	}
}

// InitiateRead creates an entry in fileInfoCache if it does not already exist.
// It creates FileDownloadJob if not already exist. Also, creates localFilePath
// which contains the downloaded content. Finally, it returns a CacheHandle that
// contains the async DownloadJob and the local file handle.
// TODO (raj-prince) to implement.
func (chr *CacheHandler) GetCacheHandle(object *gcs.MinObject, bucket gcs.Bucket, initialOffset int64) (*CacheHandle, error) {

	localFileReadHandle, err := chr.createLocalFileReadHandle(object.Name, bucket.Name())
	if err != nil {
		return nil, fmt.Errorf("error while create local-file read handle: %v", err)
	}

	// create read file handle
	// create file download job
	// add entry in the cache
	//
}

// DecrementJobRefCount decrement the reference count of clients which is dependent of
// async job. This will cancel the async job once, the count reaches to zero.
// TODO (raj-prince) to implement.
func (chr *CacheHandler) DecrementJobRefCount(object *gcs.MinObject, bucket gcs.Bucket) error {
	return nil
}

// RemoveFileFromCache removes the entry from the fileInfoCache, cancel the async running job incase,
// and delete the locally downloaded cached-file.
// TODO (raj-prince) to implement.
func (chr *CacheHandler) RemoveFileFromCache(object *gcs.MinObject, bucket gcs.Bucket) {

}

// Destroy destroys the internal state of CacheHandler correctly specifically closing any fileHandles.
// TODO (raj-prince) to implement.
func (chr *CacheHandler) Destroy() {

}
