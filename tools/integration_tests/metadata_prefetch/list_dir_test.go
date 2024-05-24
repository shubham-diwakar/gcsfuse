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

// Provides integration tests for list directory.
package metadata_prefetch

import (
	"io/fs"
	"path"
	"path/filepath"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
	"github.com/stretchr/testify/assert"
)

func createDirectoryStructureForTest(t *testing.T) string {
	/*
		dirForMetadataPrefetchListTest \
		  file-1
		  subdir1 \
		    file-1
		    file-2
		  subdir2 \
		    file-1
		  emptydir
	*/
	testDir := setup.SetupTestDirectory("dirForMetadataPrefetchListTest")

	operations.CreateDirectoryWithNFiles(1, testDir, "file-", t)
	subdirPath1 := path.Join(testDir, "subdir1")
	operations.CreateDirectoryWithNFiles(2, subdirPath1, "file-", t)

	subdirPath2 := path.Join(testDir, "subdir2")
	operations.CreateDirectoryWithNFiles(1, subdirPath2, "file-", t)

	subdirPath3 := path.Join(testDir, "emptydir")
	operations.CreateDirectoryWithNFiles(0, subdirPath3, "", t)
	return testDir
}

func TestListDirectory(t *testing.T) {
	testDir := createDirectoryStructureForTest(t)
	var filePaths []string
	err := filepath.WalkDir(testDir, func(path string, dir fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		filePaths = append(filePaths, path)
		return nil
	})

	if assert.Nil(t, err) {
		assert.ElementsMatch(t, []string{
			"dirForMetadataPrefetchListTest",
			"dirForMetadataPrefetchListTest/file-1",
			"dirForMetadataPrefetchListTest/subdir1",
			"dirForMetadataPrefetchListTest/subdir1/file-1",
			"dirForMetadataPrefetchListTest/subdir1/file-2",
			"dirForMetadataPrefetchListTest/subdir2",
			"dirForMetadataPrefetchListTest/subdir2/file-1",
			"dirForMetadataPrefetchListTest/emptydir",
		}, filePaths)
	}
}
