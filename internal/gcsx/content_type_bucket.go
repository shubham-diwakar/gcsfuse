// Copyright 2016 Google Inc. All Rights Reserved.
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

package gcsx

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/jacobsa/gcloud/gcs"
	"github.com/jacobsa/gcloud/httputil"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

// NewContentTypeBucket creates a wrapper bucket that guesses MIME types for
// newly created or composed objects when an explicit type is not already set.
func NewContentTypeBucket(b gcs.Bucket) gcs.Bucket {
	return contentTypeBucket{b}
}

type contentTypeBucket struct {
	gcs.Bucket
}

func (b contentTypeBucket) CreateObject(
	ctx context.Context,
	req *gcs.CreateObjectRequest) (o *gcs.Object, err error) {
	// Guess a content type if necessary.
	if req.ContentType == "" {
		req.ContentType = mime.TypeByExtension(path.Ext(req.Name))
	}

	// Pass on the request.
	o, err = b.Bucket.CreateObject(ctx, req)
	return
}

func (b contentTypeBucket) ComposeObjects(
	ctx context.Context,
	req *gcs.ComposeObjectsRequest) (o *gcs.Object, err error) {
	// Guess a content type if necessary.
	if req.ContentType == "" {
		req.ContentType = mime.TypeByExtension(path.Ext(req.DstName))
	}

	// Pass on the request.
	o, err = b.Bucket.ComposeObjects(ctx, req)
	return
}

func (b contentTypeBucket) NewReader(
	ctx context.Context,
	req *gcs.ReadObjectRequest) (rc io.ReadCloser, err error) {
	// Modify the request and call through.

	fmt.Println("Invoking from contenttype bucket")
	bucketSegment := httputil.EncodePathSegment("swethv-test-central")
	objectSegment := httputil.EncodePathSegment(req.Name)
	opaque := fmt.Sprintf(
		"//%s/download/storage/v1/b/%s/o/%s",
		"storage.googleapis.com:443",
		bucketSegment,
		objectSegment)

	query := make(url.Values)
	query.Set("alt", "media")

	if req.Generation != 0 {
		query.Set("generation", fmt.Sprintf("%d", req.Generation))
	}

	url := &url.URL{
		Scheme:   "https",
		Host:     "storage.googleapis.com:443",
		Opaque:   opaque,
		RawQuery: query.Encode(),
	}

	// Create an HTTP request.
	httpReq, err := httputil.NewRequest(ctx, "GET", url, nil, 0, "test")
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	if req.Range != nil {
		var v string
		v, _ = makeRangeHeaderValue(*req.Range)
		httpReq.Header.Set("Range", v)
	}

	// Call the server.
	httpRes, err := b.Bucket.GetHttpClient().Do(httpReq)
	if err != nil {
		return
	}

	// Close the body if we're returning in error.
	defer func() {
		if err != nil {
			googleapi.CloseBody(httpRes)
		}
	}()

	// Check for HTTP error statuses.
	if err = googleapi.CheckResponse(httpRes); err != nil {
		if typed, ok := err.(*googleapi.Error); ok {
			// Special case: handle not found errors.
			if typed.Code == http.StatusNotFound {
				err = &gcs.NotFoundError{Err: typed}
			}

			// Special case: if the user requested a range and we received HTTP 416
			// from the server, treat this as an empty body. See makeRangeHeaderValue
			// for more details.
			if req.Range != nil &&
				typed.Code == http.StatusRequestedRangeNotSatisfiable {
				err = nil
				googleapi.CloseBody(httpRes)
				rc = ioutil.NopCloser(strings.NewReader(""))
			}
		}

		return
	}

	// The body contains the object data.
	rc = httpRes.Body

	return
}

func makeRangeHeaderValue(br gcs.ByteRange) (hdr string, n int64) {
	// HACK(jacobsa): Above a certain number N, GCS appears to treat Range
	// headers containing a last-byte-pos > N as syntactically invalid. I've
	// experimentally determined that N is 2^63-1, which makes sense if they are
	// using signed integers.
	//
	// Since math.MaxUint64 is a reasonable way to express "infinity" for a
	// limit, and because we don't intend to support eight-exabyte objects,
	// handle this by truncating the limit. This also prevents overflow when
	// casting to int64 below.
	if br.Limit > math.MaxInt64 {
		br.Limit = math.MaxInt64
	}

	// Canonicalize ranges that the server will not like. We must do this because
	// RFC 2616 §14.35.1 requires the last byte position to be greater than or
	// equal to the first byte position.
	if br.Limit < br.Start {
		br.Start = 0
		br.Limit = 0
	}

	// HTTP byte range specifiers are [min, max] double-inclusive, ugh. But we
	// require the user to truncate, so there is no harm in requesting one byte
	// extra at the end. If the range GCS sees goes past the end of the object,
	// it truncates. If the range starts after the end of the object, it returns
	// HTTP 416, which we require the user to handle.
	hdr = fmt.Sprintf("bytes=%d-%d", br.Start, br.Limit)
	n = int64(br.Limit - br.Start)

	return
}
