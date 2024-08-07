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

// Provide test for listing large directory
package list_large_dir_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/mounting/static_mounting"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
)

const DirectoryForListLargeFileTests = "directoryForListLargeFileTests"
const DirectoryWithTwelveThousandFiles = "directoryWithTwelveThousandFiles"
const PrefixFileInDirectoryWithTwelveThousandFiles = "fileInDirectoryWithTwelveThousandFiles"
const PrefixExplicitDirInLargeDirListTest = "explicitDirInLargeDirListTest"
const PrefixImplicitDirInLargeDirListTest = "implicitDirInLargeDirListTest"
const NumberOfFilesInDirectoryWithTwelveThousandFiles = 12000
const NumberOfImplicitDirsInDirectoryWithTwelveThousandFiles = 100
const NumberOfExplicitDirsInDirectoryWithTwelveThousandFiles = 100

func TestMain(m *testing.M) {
	setup.ParseSetUpFlags()

	flags := [][]string{{"--implicit-dirs", "--stat-cache-ttl=0", "--kernel-list-cache-ttl-secs=-1"}}
	if !testing.Short() {
		flags = append(flags, []string{"--client-protocol=grpc", "--implicit-dirs=true", "--stat-cache-ttl=0"})
	}
	// Create storage client before running tests.
	ctx := context.Background()
	var storageClient *storage.Client
	closeStorageClient := client.CreateStorageClientWithTimeOut(&ctx, &storageClient, time.Minute*15)
	defer func() {
		err := closeStorageClient()
		if err != nil {
			log.Fatalf("closeStorageClient failed: %v", err)
		}
	}()

	if hnsFlagSet, err := setup.AddHNSFlagForHierarchicalBucket(ctx, storageClient); err == nil {
		flags = append(flags, hnsFlagSet)
	}

	if setup.TestBucket() == "" && setup.MountedDirectory() != "" {
		log.Print("Please pass the name of bucket mounted at mountedDirectory to --testBucket flag.")
		os.Exit(1)
	}

	// Run tests for mountedDirectory only if --mountedDirectory flag is set.
	setup.RunTestsForMountedDirectoryFlag(m)

	setup.SetUpTestDirForTestBucketFlag()

	successCode := static_mounting.RunTests(flags, m)

	os.Exit(successCode)
}
