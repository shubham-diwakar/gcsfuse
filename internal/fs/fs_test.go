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

package fs_test

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/googlecloudplatform/gcsfuse/internal/fs"
	"github.com/googlecloudplatform/gcsfuse/internal/gcsx"
	"github.com/googlecloudplatform/gcsfuse/internal/locker"
	"github.com/googlecloudplatform/gcsfuse/internal/logger"
	"github.com/googlecloudplatform/gcsfuse/internal/perms"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/gcs/gcsfake"
	"github.com/jacobsa/gcloud/gcs/gcsutil"
	. "github.com/jacobsa/ogletest"
	"github.com/jacobsa/timeutil"
	"golang.org/x/net/context"
)

const (
	filePerms      os.FileMode = 0740
	dirPerms                   = 0754
	RenameDirLimit             = 5
)

func TestFS(t *testing.T) { RunTests(t) }

var fDebug = flag.Bool("debug_fuse", false, "Print debugging output.")

// Install a SIGINT handler that exits gracefully once the current test is
// finished. It's not safe to exit in the middle of a test because closing any
// open files may require the fuse daemon to still be responsive.
func init() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	locker.EnableDebugMessages()

	go func() {
		<-c
		logger.Info("Received SIGINT; exiting after this test completes.")
		StopRunningTests()
	}()
}

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

// A struct that can be embedded to inherit common file system test behaviors.
type fsTest struct {
	ctx context.Context

	// Configuration
	serverCfg fs.ServerConfig
	mountCfg  fuse.MountConfig

	// Dependencies. If bucket is set before SetUp is called, it will be used
	// rather than creating a default one.
	mtimeClock timeutil.Clock
	cacheClock timeutil.SimulatedClock

	// To mount a special bucket, override `bucket`;
	// To mount multiple buckets, override `buckets`;
	// Otherwise, a default bucket will be used.
	bucket  gcs.Bucket
	buckets map[string]gcs.Bucket

	// Mount information
	mfs *fuse.MountedFileSystem
	Dir string

	// Files to close when tearing down. Nil entries are skipped.
	f1 *os.File
	f2 *os.File
}

var (
	mntDir string
	ctx    context.Context
	bucket gcs.Bucket

	// Mount information
	mfs *fuse.MountedFileSystem

	mtimeClock timeutil.Clock
)

var _ SetUpTestSuiteInterface = &fsTest{}
var _ TearDownTestSuiteInterface = &fsTest{}

func (t *fsTest) SetUpTestSuite() {
	var err error
	ctx = context.Background()

	// Set up the clocks.
	mtimeClock = timeutil.RealClock()
	t.cacheClock.SetTime(time.Date(2015, 4, 5, 2, 15, 0, 0, time.Local))
	t.serverCfg.CacheClock = &t.cacheClock

	if t.buckets != nil {
		// mount all buckets
		t.bucket = nil
		t.serverCfg.BucketName = ""
	} else {
		// mount a single bucket
		if t.bucket == nil {
			t.bucket = gcsfake.NewFakeBucket(mtimeClock, "some_bucket")
			bucket = t.bucket
		}
		t.serverCfg.BucketName = t.bucket.Name()
		t.buckets = map[string]gcs.Bucket{t.bucket.Name(): t.bucket}
	}

	t.serverCfg.BucketManager = &fakeBucketManager{
		// This bucket manager is allowed to open these buckets
		buckets: t.buckets,
		// Configs for the syncer when setting up buckets
		appendThreshold: 0,
		tmpObjectPrefix: ".gcsfuse_tmp/",
	}
	t.serverCfg.RenameDirLimit = RenameDirLimit
	t.serverCfg.SequentialReadSizeMb = 200

	// Set up ownership.
	t.serverCfg.Uid, t.serverCfg.Gid, err = perms.MyUserAndGroup()
	AssertEq(nil, err)

	// Set up permissions.
	t.serverCfg.FilePerms = filePerms
	t.serverCfg.DirPerms = dirPerms

	// Set up a temporary directory for mounting.
	mntDir, err = ioutil.TempDir("", "fs_test")
	AssertEq(nil, err)

	// Create a file system server.
	server, err := fs.NewServer(ctx, &t.serverCfg)
	AssertEq(nil, err)

	// Mount the file system.
	mountCfg := t.mountCfg
	mountCfg.OpContext = ctx

	if mountCfg.ErrorLogger == nil {
		mountCfg.ErrorLogger = logger.NewError("fuse_errors: ")
	}

	if *fDebug {
		mountCfg.DebugLogger = logger.NewDebug("fuse: ")
	}

	mfs, err = fuse.Mount(mntDir, server, &mountCfg)
	AssertEq(nil, err)
}

func (t *fsTest) TearDownTestSuite() {
	var err error
	// Unmount the file system. Try again on "resource busy" errors.
	delay := 10 * time.Millisecond
	for {
		err := fuse.Unmount(mfs.Dir())
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "resource busy") {
			logger.Info("Resource busy error while unmounting; trying again")
			time.Sleep(delay)
			delay = time.Duration(1.3 * float64(delay))
			continue
		}

		AddFailure("MountedFileSystem.Unmount: %v", err)
		AbortTest()
	}

	if err := mfs.Join(ctx); err != nil {
		AssertEq(nil, err)
	}

	// Unlink the mount point.
	if err = os.Remove(mntDir); err != nil {
		err = fmt.Errorf("Unlinking mount point: %w", err)
		return
	}
}

func (t *fsTest) TearDown() {
	// Close any files we opened.
	if t.f1 != nil {
		ExpectEq(nil, t.f1.Close())
	}

	if t.f2 != nil {
		ExpectEq(nil, t.f2.Close())
	}

	// Remove all contents for mntDir. This helps to keep the directory clean
	// for next test run
	os.RemoveAll(mntDir)
}

func (t *fsTest) createWithContents(name string, contents string) error {
	return t.createObjects(map[string]string{name: contents})
}

func (t *fsTest) createObjects(in map[string]string) error {
	b := make(map[string][]byte)
	for k, v := range in {
		b[k] = []byte(v)
	}

	err := gcsutil.CreateObjects(ctx, bucket, b)
	return err
}

func (t *fsTest) createEmptyObjects(names []string) error {
	err := gcsutil.CreateEmptyObjects(ctx, bucket, names)
	return err
}

////////////////////////////////////////////////////////////////////////
// Common helpers
////////////////////////////////////////////////////////////////////////

func getFileNames(entries []os.FileInfo) (names []string) {
	for _, e := range entries {
		names = append(names, e.Name())
	}

	return
}

// REQUIRES: n % 4 == 0
func randBytes(n int) (b []byte) {
	if n%4 != 0 {
		panic(fmt.Sprintf("Illegal size: %d", n))
	}

	b = make([]byte, n)
	for i := 0; i < n; i += 4 {
		u32 := rand.Uint32()
		b[i] = byte(u32 >> 0)
		b[i+1] = byte(u32 >> 8)
		b[i+2] = byte(u32 >> 16)
		b[i+3] = byte(u32 >> 24)
	}

	return
}

func readRange(r io.ReadSeeker, offset int64, n int) (s string, err error) {
	if _, err = r.Seek(offset, 0); err != nil {
		return
	}

	bytes := make([]byte, n)
	if _, err = io.ReadFull(r, bytes); err != nil {
		return
	}

	s = string(bytes)
	return
}

func currentUid() uint32 {
	user, err := user.Current()
	AssertEq(nil, err)

	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	AssertEq(nil, err)

	return uint32(uid)
}

func currentGid() uint32 {
	user, err := user.Current()
	AssertEq(nil, err)

	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	AssertEq(nil, err)

	return uint32(gid)
}

type fakeBucketManager struct {
	buckets         map[string]gcs.Bucket
	appendThreshold int64
	tmpObjectPrefix string
}

func (bm *fakeBucketManager) ShutDown() {}

func (bm *fakeBucketManager) SetUpBucket(
	ctx context.Context,
	name string) (sb gcsx.SyncerBucket, err error) {
	bucket, ok := bm.buckets[name]
	if ok {
		sb = gcsx.NewSyncerBucket(
			bm.appendThreshold,
			bm.tmpObjectPrefix,
			gcsx.NewContentTypeBucket(bucket),
		)
		return
	}
	err = fmt.Errorf("Bucket %q does not exist", name)
	return
}
