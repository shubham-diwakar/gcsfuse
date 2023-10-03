package readcache

import (
	"fmt"
	"os"

	"github.com/googlecloudplatform/gcsfuse/internal/fs/inode"
)

type FileInfo struct {
	Name       string
	Generation int64
	Offset     int64
}

type FileCache struct {
	maxTotalSize          int64
	totalSize             int64
	fileInfoCache         *FileInfoCache
	fileDownloaderManager *FileDownloaderManager
}

func (fc *FileCache) isClobbered(fileInode *inode.FileInode, fileInfo *FileInfo) bool {
	// make stat call using fileInode.Bucket().StatObject() to get the latest
	// generation (from stat cache) and then compare with generation in FileInfo
}

func (fc *FileCache) readFromFile(localFileHandle *os.File, offset int64, dst []byte) (n int, err error) {
	// need lock on localFileHandle
	_, err = localFileHandle.Seek(offset, 0)
	if err != nil {
		return
	}
	n, err = localFileHandle.Read(dst)
	return
}

func (fc *FileCache) Remove(fileName string) (err error) {
	err = fc.fileDownloaderManager.StopDownload(fileName)
	if err != nil {
		return
	}
	fileInfo, err := fc.fileInfoCache.Remove(fileName)
	if err != nil {
		return
	}
	if fileInfo != nil {
		fc.totalSize -= fileInfo.Size
	}
}

func (fc *FileCache) makeSpaceForNewFile(fileInode *inode.FileInode) (err error) {
	var fileSize int64
	// make stat call to get size of file
	if fileSize+fc.totalSize < fc.maxTotalSize {
		return
	}
	if fileSize > fc.maxTotalSize {
		err = fmt.Errorf("fileSize greater than the max size of cache")
		return
	}
	for (fc.totalSize + fc.totalSize) > fc.maxTotalSize {
		fileInfo := fc.fileInfoCache.GetLastEntry()
		err = fc.Remove(fileInfo.Name)
		if err != nil {
			return
		}
	}
}
func (fc *FileCache) Read(fileInode *inode.FileInode, localFileHandle *os.File, waitForDownload bool, dst []byte, offset int64, sequentialReadSizeMb int) (n int, err error) {
	fileInfo := fc.fileInfoCache.Get(fileInode.Name().GcsObjectName())
	if fileInfo == nil && waitForDownload {
		err = fc.makeSpaceForNewFile(fileInode)
		if err != nil {
			return
		}
		fileInfo = fc.fileInfoCache.Put(fileInode.Name(), fileInode.SourceGeneration().Object, 0)
		// need lock on fileInfo
		err = fc.fileDownloaderManager.Download(fileInode.Name().GcsObjectName(), int64(len(dst))+offset)
		if err != nil {
			return
		}
		n, err = fc.readFromFile(localFileHandle, offset, dst)
		return
	}

	if fc.isClobbered(fileInode, fileInfo) {
		err = fc.Remove(fileInode.Name().GcsObjectName())
		return
	}

	if fileInfo.Offset >= (offset + len(dst)) {
		if localFileHandle == nil {
			localFileHandle, err = os.Open(fileInfo.Name)
			if err != nil {
				return
			}
		}
		n, err = fc.readFromFile(localFileHandle, offset, dst)
		return
	}
	if !waitForDownload {
		return
	}

	err = fc.fileDownloaderManager.Download(fileInode.Name().GcsObjectName(), int64(len(dst))+offset)
	if err != nil {
		return
	}
	n, err = fc.readFromFile(localFileHandle, offset, dst)
	return
}
