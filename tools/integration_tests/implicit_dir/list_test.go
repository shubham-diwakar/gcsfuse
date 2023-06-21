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

package implicit_dir_test

import (
	"log"
	"os"
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/tools/integration_tests/util/setup"
)

func TestListImplicitObjectsFromBucket(t *testing.T) {
		setup.RunScriptForTestData("delete_objects.sh", "swethv-vertex-ai")

		setup.RunScriptForTestData("create_objects.sh", "swethv-vertex-ai")

		err := os.Mkdir("/home/swethv_google_com/gcs/implicitDirectory/A", 777)
		if err != nil {
			log.Printf("Error in creating directory: %v", err)
		}

		filePath := path.Join("/home/swethv_google_com/gcs/implicitDirectory/A/a.txt")
		_, err = os.Create(filePath)
		if err != nil {
			log.Printf("Create file at : %v", err)
		}

		os.RemoveAll("/home/swethv_google_com/gcs/implicitDirectory")

		setup.RunScriptForTestData("delete_objects.sh", "swethv-vertex-ai")

		setup.RunScriptForTestData("create_objects.sh", "swethv-vertex-ai")

		_, err = os.Stat("/home/swethv_google_com/gcs/implicitDirectory/implicitSubDirectory/")
		if err != nil {
			log.Printf("Stating file at : %v", err)
		}
	}
}
