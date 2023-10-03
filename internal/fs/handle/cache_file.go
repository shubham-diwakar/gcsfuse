// Copyright 2015 Google Inc. All Rights Reserved.
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

package handle

import (
	"os"

	"github.com/googlecloudplatform/gcsfuse/internal/fs/inode"
	"github.com/jacobsa/syncutil"
	"golang.org/x/net/context"
)

type CacheFileHandle struct {
	fileHandle *FileHandle
	mu         syncutil.InvariantMutex
	lastOffset int64
	isRandom   bool
	//fileCache *FileCache
	cacheForRandomRead bool
	localFileHandle    *os.File
}

func NewCacheFileHandle(fileHandle *FileHandle) (cacheFileHandle *CacheFileHandle) {
	cacheFileHandle = &CacheFileHandle{
		fileHandle: fileHandle,
		lastOffset: 0,
	}

	//cacheFileHandle.mu = syncutil.NewInvariantMutex(cacheFileHandle.checkInvariants)

	return
}

func (fh *CacheFileHandle) FullObjectName() string {
	s := ""
	return s
}

func (fh *CacheFileHandle) ObjectSize() int64 {
	var s int64 = 0
	return s
}

// Destroy any resources associated with the handle, which must not be used
// again.
func (fh *CacheFileHandle) Destroy() {
	fh.fileHandle.Destroy()
}

// Inode returns the inode backing this handle.
func (fh *CacheFileHandle) Inode() *inode.FileInode {
	return fh.fileHandle.inode
}

func (fh *CacheFileHandle) Lock() {
	fh.mu.Lock()
	fh.fileHandle.Lock()
}

func (fh *CacheFileHandle) Unlock() {
	fh.mu.Unlock()
	fh.fileHandle.Unlock()
}

// Equivalent to locking fh.Inode() and calling fh.Inode().Read, but may be
// more efficient.
//
// LOCKS_REQUIRED(fh)
// LOCKS_EXCLUDED(fh.inode)
func (fh *CacheFileHandle) Read(ctx context.Context, dst []byte, offset int64, sequentialReadSizeMb int32) (n int, err error) {
	// Lock the inode and attempt to ensure that we have a reader for its current
	// state, or clear fh.reader if it's not possible to create one (probably
	// because the inode is dirty).
	// if file is dirty then read from fileHandle
	// n, err = fh.fileHandle.Read(ctx, dst, offset, sequentialReadSizeMb)
	// return
	if fh.isRandom != true && (offset != fh.lastOffset) {
		if fh.lastOffset != 0 {
			fh.fileCache.ReduceSequentialRead()
		}
		fh.isRandom = true
	}
	var waitForDownload bool = !fh.isRandom
	n, err = fh.fileCache.Read(fh.Inode(), fh.localFileHandle, waitForDownload, dst, offset, sequentialReadSizeMb)

}
