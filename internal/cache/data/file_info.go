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

package data

import (
	"errors"
	"fmt"
	"os"
	"time"
)

const InvalidKeyAttributes = "key attributes not initialised"

const TestTimeInEpoch int64 = 1654041600

type FileInfoKey struct {
	BucketName         string
	BucketCreationTime time.Time
	ObjectName         string
}

// Key will return a string, combining all the attributes of FileInfoKey.
// Returns error in case of uninitialized value.
func (fik FileInfoKey) Key() (string, error) {
	if fik.BucketName == "" || fik.ObjectName == "" {
		return "", errors.New(InvalidKeyAttributes)
	}
	unixTimeString := fmt.Sprintf("%d", fik.BucketCreationTime.Unix())
	return fik.BucketName + unixTimeString + fik.ObjectName, nil
}

type FileInfo struct {
	Key              FileInfoKey
	ObjectGeneration int64
	Offset           uint64
	FileSize         uint64
}

func (fi FileInfo) Size() uint64 {
	return fi.FileSize
}

type FileSpec struct {
	Path string
	Perm os.FileMode
}
