// Copyright 2022 Google Inc. All Rights Reserved.
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

package storage

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/googlecloudplatform/gcsfuse/internal/gorocksdb"
	"github.com/googlecloudplatform/gcsfuse/internal/logger"
	mountpkg "github.com/googlecloudplatform/gcsfuse/internal/mount"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/storageutil"
	"github.com/jacobsa/syncutil"
	"github.com/spenczar/tdigest"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

type StorageHandle interface {
	// In case of non-empty billingProject, this project is set as user-project for
	// all subsequent calls on the bucket. Calls with user-project will be billed
	// to that project rather than to the bucket's owning project.
	//
	// A user-project is required for all operations on Requester Pays buckets.
	BucketHandle(bucketName string, billingProject string) (bh *bucketHandle)

	GetMetadataObjects() []*MinObject
	WriteToDb(items []*MinObject) (err error)
	ReadData(ctx context.Context, items []*MinObject) (err error)
}

type storageClient struct {
	client *storage.Client
	db     *gorocksdb.DB
	bh     *bucketHandle
}

type StorageClientConfig struct {
	ClientProtocol      mountpkg.ClientProtocol
	MaxConnsPerHost     int
	MaxIdleConnsPerHost int
	TokenSrc            oauth2.TokenSource
	HttpClientTimeout   time.Duration
	MaxRetryDuration    time.Duration
	RetryMultiplier     float64
	UserAgent           string
}

// NewStorageHandle returns the handle of Go storage client containing
// customized http client. We can configure the http client using the
// storageClientConfig parameter.
func NewStorageHandle(ctx context.Context, clientConfig StorageClientConfig) (sh StorageHandle, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if clientConfig.ClientProtocol == mountpkg.HTTP1 {
		transport = &http.Transport{
			MaxConnsPerHost:     clientConfig.MaxConnsPerHost,
			MaxIdleConnsPerHost: clientConfig.MaxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   clientConfig.MaxConnsPerHost,
			ForceAttemptHTTP2: true,
		}
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: clientConfig.TokenSrc,
		},
		Timeout: clientConfig.HttpClientTimeout,
	}

	// Setting UserAgent through RoundTripper middleware
	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: clientConfig.UserAgent,
	}
	var sc *storage.Client
	sc, err = storage.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		err = fmt.Errorf("go storage client creation failed: %w", err)
		return
	}

	// ShouldRetry function checks if an operation should be retried based on the
	// response of operation (error.Code).
	// RetryAlways causes all operations to be checked for retries using
	// ShouldRetry function.
	// Without RetryAlways, only those operations are checked for retries which
	// are idempotent.
	// https://github.com/googleapis/google-cloud-go/blob/main/storage/storage.go#L1953
	sc.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        clientConfig.MaxRetryDuration,
			Multiplier: clientConfig.RetryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(storageutil.ShouldRetry))

	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetUseFsync(true)
	db, err := gorocksdb.OpenDb(options, "//mnt/disks/local_ssd_0/test1m")
	if err != nil {
		fmt.Println("error in initializing rocksdb")
		fmt.Println(err)
	}

	writeOptions := gorocksdb.NewDefaultWriteOptions()
	key := "testkey"
	value := "testvalue"
	err = db.Put(writeOptions, []byte(key), []byte(value))
	if err != nil {
		fmt.Println("error in initializing rocksdb")
		fmt.Println(err)
	}

	/*ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte("swethv_ls_metrics1KB_1000000files_0subdir/file_1.txt"))
	i := 1
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		if i == 1 {
			fmt.Printf("Key: %v Value: %v\n", string(key.Data()), string(value.Data()))
		}
		i++
		key.Free()
		value.Free()
	}

	fmt.Println(i)*/

	/*	writeOptions := gorocksdb.NewDefaultWriteOptions()
		key := "testkey"
		value := "testvalue"
		err = db.Put(writeOptions, []byte(key), []byte(value))
		if err != nil {
			fmt.Println("error in initializing rocksdb")
			fmt.Println(err)
		}

		readOptions := gorocksdb.NewDefaultReadOptions()

		slice, err := db.Get(readOptions, []byte(key))
		if err != nil && value == string(slice.Data()) {
			fmt.Println("value matched")
		} else {
			fmt.Println("value didnt match")
			fmt.Println(string(slice.Data()))
			fmt.Print(err)
		}*/

	sh = &storageClient{client: sc, db: db}
	return
}

func (sh *storageClient) BucketHandle(bucketName string, billingProject string) (bh *bucketHandle) {
	storageBucketHandle := sh.client.Bucket(bucketName)

	if billingProject != "" {
		storageBucketHandle = storageBucketHandle.UserProject(billingProject)
	}

	bh = &bucketHandle{bucket: storageBucketHandle, bucketName: bucketName}
	sh.bh = bh
	return
}

func (sh *storageClient) GetMetadataObjects() []*MinObject {
	return sh.bh.ObjectsToCache
}

func (sh *storageClient) ReadData(ctx context.Context, items []*MinObject) (err error) {
	length := 1000000
	names := [1000000]string{}
	index := 0
	for _, ele := range items {
		names[index] = sh.bh.bucketName + ele.Name

		index++
		if index >= length {
			break
		}
	}

	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	var mean float64
	notfound := 0

	td := tdigest.New()
	b := syncutil.NewBundle(ctx)
	for i := 0; i < 10; i++ {

		b.Add(func(ctx context.Context) (err error) {

			found := 0
			//	sub_slice := names[i*10000 : (i+1)*10000]
			sub_slice := names[i*10000 : (i+1)*10000]
			for j := 0; j < 10000; j++ {
				if sub_slice[j] == "" {
					continue
				}

				found++

				start := time.Now()

				output, err1 := sh.db.Get(ro, []byte(sub_slice[j]))
				if err1 != nil {
					err = err1
					return
				}

				elapsed := time.Since(start)
				//fmt.Println(elapsed)
				microseconds := float64(elapsed) / float64(time.Microsecond)
				//	fmt.Println(microseconds)
				td.Add(microseconds, 1)
				mean += microseconds

				if output.Size() == 0 {
					/*fmt.Println("key not found")
					fmt.Println(key)*/
					notfound++
					//err = fmt.Errorf("key not found %s", key)
				} else {
					output.Free()
				}
			}

			fmt.Printf("Found %d\n", found)

			return
		})
	}

	if err = b.Join(); err != nil {
		return
	}

	fmt.Println("not found %d", notfound)
	fmt.Printf("Mean %.5f\n", mean/float64(length))
	fmt.Printf("50th: %.5f\n", td.Quantile(0.5))
	fmt.Printf("90th: %.5f\n", td.Quantile(0.9))
	fmt.Printf("99th: %.5f\n", td.Quantile(0.99))
	fmt.Printf("99.9th: %.5f\n", td.Quantile(0.999))
	fmt.Printf("99.99th: %.5f\n", td.Quantile(0.9999))

	return

}

func (sh *storageClient) WriteToDb(items []*MinObject) (err error) {
	fmt.Println(len(items))
	i := 1
	writeOptions := gorocksdb.NewDefaultWriteOptions()
	for _, ele := range items {
		j, err1 := json.MarshalIndent(ele, "", " ")
		if err1 != nil {
			logger.Info("received error %s", err1)
			return
		}

		if i == 1 {
			fmt.Println(sh.bh.bucketName + ele.Name)

		}

		i++

		err = sh.db.Put(writeOptions, []byte(sh.bh.bucketName+ele.Name), j)

		if err != nil {
			fmt.Println("Received error")
			fmt.Println(err)
			return
		}
	}

	fmt.Println("Total eelements")
	fmt.Println(i)

	return
}
