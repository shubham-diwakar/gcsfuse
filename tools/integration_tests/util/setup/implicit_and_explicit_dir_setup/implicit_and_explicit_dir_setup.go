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

package implicit_and_explicit_dir_setup

import (
	"log"
	"os"
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/mounting/persistent_mounting"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/mounting/static_mounting"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup"
)

const ExplicitDirectory = "explicitDirectory"
const ExplicitFile = "explicitFile"
const ImplicitDirectory = "implicitDirectory"
const ImplicitSubDirectory = "implicitSubDirectory"
const NumberOfExplicitObjects = 2
const NumberOfTotalObjects = 3
const NumberOfFilesInExplicitDirectory = 2
const NumberOfFilesInImplicitDirectory = 2
const NumberOfFilesInImplicitSubDirectory = 1
const PrefixFileInExplicitDirectory = "fileInExplicitDir"
const FirstFileInExplicitDirectory = "fileInExplicitDir1"
const SecondFileInExplicitDirectory = "fileInExplicitDir2"
const FileInImplicitDirectory = "fileInImplicitDir1"
const FileInImplicitSubDirectory = "fileInImplicitDir2"
const GzipEncode = false

func RunTestsForImplicitDirAndExplicitDir(flags [][]string, m *testing.M) {
	setup.ParseSetUpFlags()

	setup.ExitWithFailureIfBothTestBucketAndMountedDirectoryFlagsAreNotSet()

	if setup.TestBucket() == "" && setup.MountedDirectory() != "" {
		log.Print("Please pass the name of bucket mounted at mountedDirectory to --testBucket flag.")
		os.Exit(1)
	}

	// Run tests for mountedDirectory only if --mountedDirectory and --testbucket flag is set.
	setup.RunTestsForMountedDirectoryFlag(m)

	// Run tests for testBucket only if --testbucket flag is set.
	setup.SetUpTestDirForTestBucketFlag()

	successCode := static_mounting.RunTests(flags, m)

	if successCode == 0 {
		successCode = persistent_mounting.RunTests(flags, m)
	}

	setup.RemoveBinFileCopiedForTesting()

	os.Exit(successCode)
}

func RemoveAndCheckIfDirIsDeleted(dirPath string, dirName string, t *testing.T) {
	operations.RemoveDir(dirPath)

	dir, err := os.Stat(dirPath)
	if err == nil && dir.Name() == dirName && dir.IsDir() {
		t.Errorf("Directory is not deleted.")
	}
}

// Create file in /tmp directory, write given content
func createAndUploadImplicitFilesInBucket(fileName, gcsDirPath string) {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Error in writing file:%v", err)
	}

	defer operations.CloseFile(f)
	defer operations.RemoveFile(fileName)

	filePathInGcsBucket := path.Join(gcsDirPath, fileName)
	err = operations.UploadGcsObject(fileName, filePathInGcsBucket, GzipEncode)
	if err != nil {
		log.Fatalf("Error in uploading file in gcs bucket:%v", err)
	}
}

func CreateImplicitDirectoryStructure(testDirName string) {
	// Implicit Directory Structure
	// testBucket/testDirName/implicitDirectory                                                  -- Dir
	// testBucket/testDirName/implicitDirectory/fileInImplicitDir1                               -- File
	// testBucket/testDirName/implicitDirectory/implicitSubDirectory                             -- Dir
	// testBucket/testDirName/implicitDirectory/implicitSubDirectory/fileInImplicitDir2          -- File

	dirInBucket := path.Join(setup.TestBucket(), testDirName)

	filePathInTestBucket := path.Join(dirInBucket, ImplicitDirectory)
	createAndUploadImplicitFilesInBucket(FileInImplicitDirectory, filePathInTestBucket)

	filePathInTestBucket = path.Join(dirInBucket, ImplicitDirectory, ImplicitSubDirectory)
	createAndUploadImplicitFilesInBucket(FileInImplicitSubDirectory, filePathInTestBucket)
}

func CreateExplicitDirectoryStructure(testDirName string, t *testing.T) {
	// Explicit Directory structure
	// testBucket/testDirName/explicitDirectory                            -- Dir
	// testBucket/testDirName/explictFile                                  -- File
	// testBucket/testDirName/explicitDirectory/fileInExplicitDir1         -- File
	// testBucket/testDirName/explicitDirectory/fileInExplicitDir2         -- File

	dirPath := path.Join(setup.MntDir(), testDirName, ExplicitDirectory)
	operations.CreateDirectoryWithNFiles(NumberOfFilesInExplicitDirectory, dirPath, PrefixFileInExplicitDirectory, t)
	filePath := path.Join(setup.MntDir(), testDirName, ExplicitFile)
	file, err := os.Create(filePath)
	if err != nil {
		t.Errorf("Create file at %q: %v", dirPath, err)
	}

	// Closing file at the end.
	defer operations.CloseFile(file)
}

func CreateImplicitDirectoryInExplicitDirectoryStructure(testDirName string, t *testing.T) {
	// testBucket/testDirName/explicitDirectory                                                                   -- Dir
	// testBucket/testDirName/explictFile                                                                         -- File
	// testBucket/testDirName/explicitDirectory/fileInExplicitDir1                                                -- File
	// testBucket/testDirName/explicitDirectory/fileInExplicitDir2                                                -- File
	// testBucket/testDirName/explicitDirectory/implicitDirectory                                                 -- Dir
	// testBucket/testDirName/explicitDirectory/implicitDirectory/fileInImplicitDir1                              -- File
	// testBucket/testDirName/explicitDirectory/implicitDirectory/implicitSubDirectory                            -- Dir
	// testBucket/testDirName/explicitDirectory/implicitDirectory/implicitSubDirectory/fileInImplicitDir2         -- File

	CreateExplicitDirectoryStructure(testDirName, t)

	dirInBucket := path.Join(setup.TestBucket(), testDirName, ExplicitDirectory)

	filePathInTestBucket := path.Join(dirInBucket, ImplicitDirectory)
	createAndUploadImplicitFilesInBucket(FileInImplicitDirectory, filePathInTestBucket)

	filePathInTestBucket = path.Join(dirInBucket, ImplicitDirectory, ImplicitSubDirectory)
	createAndUploadImplicitFilesInBucket(FileInImplicitSubDirectory, filePathInTestBucket)
}
