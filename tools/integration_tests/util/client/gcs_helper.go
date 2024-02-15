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

package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/operations"
)

const (
	FileName1          = "foo1"
	FileName2          = "foo2"
	FileName3          = "foo3"
	ExplicitDirName    = "explicit"
	ExplicitFileName1  = "explicitFile1"
	ImplicitDirName    = "implicit"
	ImplicitFileName1  = "implicitFile1"
	FileContents       = "testString"
	SizeOfFileContents = 10
	GCSFileContent     = "GCSteststring"
	GCSFileSize        = 13
	FilePerms          = 0644
	ReadSize           = 1024
	SizeTruncate       = 5
	NewFileName        = "newName"
	NewDirName         = "newDirName"
)

func CreateImplicitDir(ctx context.Context, storageClient *storage.Client,
	testDirName string, t *testing.T) {
	err := CreateObjectOnGCS(
		ctx,
		storageClient,
		path.Join(testDirName, ImplicitDirName, ImplicitFileName1),
		GCSFileContent)
	if err != nil {
		t.Errorf("Error while creating implicit directory, err: %v", err)
	}
}

func ValidateObjectNotFoundErrOnGCS(ctx context.Context, storageClient *storage.Client,
	testDirName string, fileName string, t *testing.T) {
	_, err := ReadObjectFromGCS(ctx, storageClient, path.Join(testDirName, fileName), ReadSize)
	if err == nil || !strings.Contains(err.Error(), "storage: object doesn't exist") {
		t.Fatalf("Incorrect error returned from GCS for file %s: %v", fileName, err)
	}
}

func ValidateObjectContentsFromGCS(ctx context.Context, storageClient *storage.Client,
	testDirName string, fileName string, expectedContent string, t *testing.T) {
	gotContent, err := ReadObjectFromGCS(ctx, storageClient, path.Join(testDirName, fileName), ReadSize)
	if err != nil {
		t.Fatalf("Error while reading synced local file from GCS, Err: %v", err)
	}

	if expectedContent != string(gotContent) {
		t.Fatalf("GCS file %s content mismatch. Got: %s, Expected: %s ", fileName, gotContent, expectedContent)
	}
}

func CloseFileAndValidateContentFromGCS(ctx context.Context, storageClient *storage.Client,
	fh *os.File, testDirName, fileName, content string, t *testing.T) {
	operations.CloseFileShouldNotThrowError(fh, t)
	ValidateObjectContentsFromGCS(ctx, storageClient, testDirName, fileName, content, t)
}

func CreateLocalFileInTestDir(ctx context.Context, storageClient *storage.Client,
	testDirPath, fileName string, t *testing.T) (string, *os.File) {
	filePath := path.Join(testDirPath, fileName)
	fh := operations.CreateFile(filePath, FilePerms, t)
	testDirName := GetDirName(testDirPath)
	ValidateObjectNotFoundErrOnGCS(ctx, storageClient, testDirName, fileName, t)
	return filePath, fh
}

func GetDirName(testDirPath string) string {
	dirName := testDirPath[strings.LastIndex(testDirPath, "/")+1:]
	return dirName
}

func CreateObjectInGCSTestDir(ctx context.Context, storageClient *storage.Client,
	testDirName, fileName, content string, t *testing.T) {
	objectName := path.Join(testDirName, fileName)
	err := CreateObjectOnGCS(ctx, storageClient, objectName, content)
	if err != nil {
		t.Fatalf("Create Object %s on GCS: %v.", objectName, err)
	}
}

func createStorageClient(ctx *context.Context, storageClient **storage.Client, t *testing.T) func() {
	var err error
	var cancel context.CancelFunc
	*ctx, cancel = context.WithTimeout(*ctx, time.Minute*15)
	*storageClient, err = CreateStorageClient(*ctx)
	if err != nil {
		log.Fatalf("client.CreateStorageClient: %v", err)
	}
	// return func to close storage client and release resources.
	return func() {
		err := (*storageClient).Close()
		if err != nil {
			t.Errorf("Failed to close storage client")
		}
		defer cancel()
	}
}

func ReadLargeFileFromGCS(gcsObjPath ,localPath string, size int64, t *testing.T) (data []byte, err error) {
	ctx:= context.Background()
	var storageClient *storage.Client
	closeStorageClient := createStorageClient(&ctx, &storageClient, t)
	defer closeStorageClient()

	data, err = ReadObjectFromGCS(ctx,storageClient,gcsObjPath,size)
	if err != nil {
		err = fmt.Errorf("Error in reading object from gcs: %v",err)
	}

	return data,err
}

// downloadFile downloads an object to a file.
func DownloadFile(w io.Writer, bucket, object string, destFileName string) error {
	// bucket := "bucket-name"
	// object := "object-name"
	// destFileName := "file.txt"
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	f, err := os.Create(destFileName)
	if err != nil {
		return fmt.Errorf("os.Create: %w", err)
	}

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("f.Close: %w", err)
	}

	fmt.Println(w, "Blob %v downloaded to local file %v\n", object, destFileName)

	return nil

}