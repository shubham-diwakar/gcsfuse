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

// Provide test for deleting implicit directory.
package implicit_dir_test

import (
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup/implicit_and_explicit_dir_setup"
)

const DirectoryForImplicitDirDeleteTesting = "directoryForImplicitDirDeleteTesting"

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory                                                  -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/fileInImplicitDir1                               -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory                             -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/fileInImplicitDir2          -- File
func TestDeleteNonEmptyImplicitDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting)

	dirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(dirPath, implicit_and_explicit_dir_setup.ImplicitDirectory, t)
}

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory                                                  -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/fileInImplicitDir1                               -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory                             -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/fileInImplicitDir2          -- File
func TestDeleteNonEmptyImplicitSubDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting)

	subDirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory, implicit_and_explicit_dir_setup.ImplicitSubDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(subDirPath, implicit_and_explicit_dir_setup.ImplicitSubDirectory, t)
}

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory                                                                    -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/explicitDirInImplicitDir                                           -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/explicitDirInImplicitDir/fileInExplicitDirInImplicitDir            -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/fileInImplicitDir1                                                 -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory                                               -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/fileInImplicitDir2                            -- File
func TestDeleteImplicitDirWithExplicitSubDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting)

	explicitDirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory, ExplicitDirInImplicitDir)

	operations.CreateDirectoryWithNFiles(NumberOfFilesInExplicitDirInImplicitDir, explicitDirPath, PrefixFileInExplicitDirInImplicitDir, t)

	dirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(dirPath, implicit_and_explicit_dir_setup.ImplicitDirectory, t)
}

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory                                                                                         -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/fileInImplicitDir1                                                                      -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory                                                                    -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/fileInImplicitDir2                                                 -- File
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/explicitDirInImplicitDir                                           -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/implicitDirectory/implicitSubDirectory/explicitDirInImplicitDir/fileInExplicitDirInImplicitDir            -- File
func TestDeleteImplicitDirWithImplicitSubDirContainingExplicitDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting)
	explicitDirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory, implicit_and_explicit_dir_setup.ImplicitSubDirectory, ExplicitDirInImplicitSubDir)

	operations.CreateDirectoryWithNFiles(NumberOfFilesInExplicitDirInImplicitSubDir, explicitDirPath, PrefixFileInExplicitDirInImplicitSubDir, t)

	dirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ImplicitDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(dirPath, implicit_and_explicit_dir_setup.ImplicitDirectory, t)
}

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory                                                                   -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explictFile                                                                         -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/fileInExplicitDir1                                                -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/fileInExplicitDir2                                                -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory                                                 -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/fileInImplicitDir1                              -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/implicitSubDirectory                            -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/implicitSubDirectory/fileInImplicitDir2         -- File
func TestDeleteImplicitDirInExplicitDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryInExplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting, t)

	dirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ExplicitDirectory, implicit_and_explicit_dir_setup.ImplicitDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(dirPath, implicit_and_explicit_dir_setup.ImplicitDirectory, t)
}

// Directory Structure
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory                                                                   -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explictFile                                                                         -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/fileInExplicitDir1                                                -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/fileInExplicitDir2                                                -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory                                                 -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/fileInImplicitDir1                              -- File
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/implicitSubDirectory                            -- Dir
// testBucket/directoryForImplicitDirDeleteTesting/explicitDirectory/implicitDirectory/implicitSubDirectory/fileInImplicitDir2         -- File
func TestDeleteExplicitDirContainingImplicitSubDir(t *testing.T) {
	// Clean the test Directory before running test.
	setup.PreTestSetup(DirectoryForImplicitDirDeleteTesting)

	// Clean the test Directory after running test.
	testDir := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting)
	defer setup.CleanUpDir(testDir)

	implicit_and_explicit_dir_setup.CreateImplicitDirectoryInExplicitDirectoryStructure(DirectoryForImplicitDirDeleteTesting, t)

	dirPath := path.Join(setup.MntDir(), DirectoryForImplicitDirDeleteTesting, implicit_and_explicit_dir_setup.ExplicitDirectory)

	implicit_and_explicit_dir_setup.RemoveAndCheckIfDirIsDeleted(dirPath, implicit_and_explicit_dir_setup.ExplicitDirectory, t)
}
