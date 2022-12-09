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

// For now, we are not writing the unit test, which requires multiple
// version of same object. As this is not supported by fake-storage-server.
// Although API is exposed to enable the object versioning for a bucket,
// but it returns "method not allowed" when we call it.

package storage

import (
	"fmt"
	"io"
	"math"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/internal/storage/storageutil"
	"github.com/jacobsa/gcloud/gcs"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type bucketHandle struct {
	wrapped    gcs.Bucket
	bucket     *storage.BucketHandle
	bucketName string
	httpClient *http.Client
}

func (bh *bucketHandle) Name() string {
	return bh.bucketName

}

func (bh *bucketHandle) NewReader(
	ctx context.Context,
	req *gcs.ReadObjectRequest) (rc io.ReadCloser, err error) {
	fmt.Println("Calling via buckethandle")
	rc, err = bh.wrapped.NewReader(ctx, req)
	return
	// Construct an appropriate URL.
	//
	// The documentation (https://goo.gl/9zeA98) is vague about how this is
	// supposed to work. As of 2015-05-14, it has no prose but gives the example:
	//
	//     www.googleapis.com/download/storage/v1/b/<bucket>/o/<object>?alt=media
	//
	// In Google-internal bug 19718068, it was clarified that the intent is that
	// each of the bucket and object names are encoded into a single path
	// segment, as defined by RFC 3986.
	/*fmt.Println("before bucket segment")
	bucketSegment := httputil.EncodePathSegment("swethv-test-central")
	fmt.Println("before object")
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

	fmt.Println("before http request")
	// Create an HTTP request.
	httpReq, err := httputil.NewRequest(ctx, "GET", url, nil, 0, "test")
	fmt.Println("after http request")
	if err != nil {
		err = fmt.Errorf("httputil.NewRequest: %v", err)
		return
	}

	// Set a Range header, if appropriate.
	var bodyLimit int64
	if req.Range != nil {
		var v string
		v, bodyLimit = makeRangeHeaderValue(*req.Range)
		httpReq.Header.Set("Range", v)
		fmt.Println("Printing range")
		fmt.Println(v)
	}

	// Call the server.
	startTime := time.Now()
	httpRes, err := bh.httpClient.Do(httpReq)
	latencyUs := time.Since(startTime).Microseconds()
	latencyMs := float64(latencyUs) / 1000.0
	fmt.Printf("Time for jacobsa: %g\n", latencyMs)
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

	// If the user requested a range and we didn't see HTTP 416 above, we require
	// an HTTP 206 response and must truncate the body. See the notes on
	// makeRangeHeaderValue.
	if req.Range != nil {
		if httpRes.StatusCode != http.StatusPartialContent {
			err = fmt.Errorf(
				"Received unexpected status code %d instead of HTTP 206",
				httpRes.StatusCode)

			return
		}

		rc = io.NopCloser(gcs.NewLimitReadCloser(rc, bodyLimit))
	}

	return
	*/
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

func (b *bucketHandle) DeleteObject(ctx context.Context, req *gcs.DeleteObjectRequest) error {
	obj := b.bucket.Object(req.Name)

	// Switching to the requested generation of the object.
	if req.Generation != 0 {
		obj = obj.Generation(req.Generation)
	}
	// Putting condition that the object's MetaGeneration should match the requested MetaGeneration for deletion to occur.
	if req.MetaGenerationPrecondition != nil && *req.MetaGenerationPrecondition != 0 {
		obj = obj.If(storage.Conditions{MetagenerationMatch: *req.MetaGenerationPrecondition})
	}

	return obj.Delete(ctx)
}

func (b *bucketHandle) StatObject(ctx context.Context, req *gcs.StatObjectRequest) (o *gcs.Object, err error) {
	var attrs *storage.ObjectAttrs
	// Retrieving object attrs through Go Storage Client.
	attrs, err = b.bucket.Object(req.Name).Attrs(ctx)

	// If error is of type storage.ErrObjectNotExist
	if err == storage.ErrObjectNotExist {
		err = &gcs.NotFoundError{Err: err} // Special case error that object not found in the bucket.
		return
	}
	if err != nil {
		err = fmt.Errorf("Error in fetching object attributes: %v", err)
		return
	}

	// Converting attrs to type *Object
	o = storageutil.ObjectAttrsToBucketObject(attrs)

	return
}

func (bh *bucketHandle) CreateObject(ctx context.Context, req *gcs.CreateObjectRequest) (o *gcs.Object, err error) {
	obj := bh.bucket.Object(req.Name)

	// GenerationPrecondition - If non-nil, the object will be created/overwritten
	// only if the current generation for the object name is equal to the given value.
	// Zero means the object does not exist.
	if req.GenerationPrecondition != nil && *req.GenerationPrecondition != 0 {
		obj = obj.If(storage.Conditions{GenerationMatch: *req.GenerationPrecondition})
	}

	// MetaGenerationPrecondition - If non-nil, the object will be created/overwritten
	// only if the current metaGeneration for the object name is equal to the given value.
	// Zero means the object does not exist.
	if req.MetaGenerationPrecondition != nil && *req.MetaGenerationPrecondition != 0 {
		obj = obj.If(storage.Conditions{MetagenerationMatch: *req.MetaGenerationPrecondition})
	}

	// Creating a NewWriter with requested attributes, using Go Storage Client.
	// Chuck size for resumable upload is default i.e. 16MB.
	wc := obj.NewWriter(ctx)
	wc = storageutil.SetAttrsInWriter(wc, req)

	// Copy the contents to the writer.
	if _, err = io.Copy(wc, req.Contents); err != nil {
		err = fmt.Errorf("error in io.Copy: %w", err)
		return
	}

	// We can't use defer to close the writer, because we need to close the
	// writer successfully before calling Attrs() method of writer.
	if err = wc.Close(); err != nil {
		err = fmt.Errorf("error in closing writer: %v", err)
		return
	}

	attrs := wc.Attrs() // Retrieving the attributes of the created object.
	// Converting attrs to type *Object.
	o = storageutil.ObjectAttrsToBucketObject(attrs)
	return
}

func (b *bucketHandle) CopyObject(ctx context.Context, req *gcs.CopyObjectRequest) (o *gcs.Object, err error) {
	srcObj := b.bucket.Object(req.SrcName)
	dstObj := b.bucket.Object(req.DstName)

	// Switching to the requested generation of source object.
	if req.SrcGeneration != 0 {
		srcObj = srcObj.Generation(req.SrcGeneration)
	}

	// Putting a condition that the metaGeneration of source should match *req.SrcMetaGenerationPrecondition for copy operation to occur.
	if req.SrcMetaGenerationPrecondition != nil {
		srcObj = srcObj.If(storage.Conditions{MetagenerationMatch: *req.SrcMetaGenerationPrecondition})
	}

	objAttrs, err := dstObj.CopierFrom(srcObj).Run(ctx)

	if err != nil {
		switch ee := err.(type) {
		case *googleapi.Error:
			if ee.Code == http.StatusPreconditionFailed {
				err = &gcs.PreconditionError{Err: ee}
			}
			if ee.Code == http.StatusNotFound {
				err = &gcs.NotFoundError{Err: storage.ErrObjectNotExist}
			}
		default:
			err = fmt.Errorf("Error in copying object: %w", err)
		}
		return
	}
	// Converting objAttrs to type *Object
	o = storageutil.ObjectAttrsToBucketObject(objAttrs)
	return
}

func getProjectionValue(req gcs.Projection) storage.Projection {
	// Explicitly converting Projection Value because the ProjectionVal interface of jacobsa/gcloud and Go Client API are not coupled correctly.
	var convertedProjection storage.Projection // Stores the Projection Value according to the Go Client API Interface.
	switch int(req) {
	// Projection Value 0 in jacobsa/gcloud maps to Projection Value 1 in Go Client API, that is for "full".
	case 0:
		convertedProjection = storage.Projection(1)
	// Projection Value 1 in jacobsa/gcloud maps to Projection Value 2 in Go Client API, that is for "noAcl".
	case 1:
		convertedProjection = storage.Projection(2)
	// Default Projection value in jacobsa/gcloud library is 0 that maps to 1 in Go Client API interface, and that is for "full".
	default:
		convertedProjection = storage.Projection(1)
	}
	return convertedProjection
}

func (b *bucketHandle) ListObjects(ctx context.Context, req *gcs.ListObjectsRequest) (listing *gcs.Listing, err error) {
	// Converting *ListObjectsRequest to type *storage.Query as expected by the Go Storage Client.
	query := &storage.Query{
		Delimiter:                req.Delimiter,
		Prefix:                   req.Prefix,
		Projection:               getProjectionValue(req.ProjectionVal),
		IncludeTrailingDelimiter: req.IncludeTrailingDelimiter,
		//MaxResults: , (Field not present in storage.Query of Go Storage Library but present in ListObjectsQuery in Jacobsa code.)
	}
	itr := b.bucket.Objects(ctx, query) // Returning iterator to the list of objects.
	pi := itr.PageInfo()
	pi.MaxSize = req.MaxResults
	pi.Token = req.ContinuationToken
	var list gcs.Listing

	// Iterating through all the objects in the bucket and one by one adding them to the list.
	for {
		var attrs *storage.ObjectAttrs
		// itr.next returns all the objects present in the bucket. Hence adding a check to break after required number of objects are returned.
		if len(list.Objects) == req.MaxResults {
			break
		}
		attrs, err = itr.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = fmt.Errorf("Error in iterating through objects: %v", err)
			return
		}

		// Prefix attribute will be set for the objects returned as part of Prefix[] array in list response.
		// https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/vendor/cloud.google.com/go/storage/storage.go#L1304
		// https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/vendor/cloud.google.com/go/storage/http_client.go#L370
		if attrs.Prefix != "" {
			list.CollapsedRuns = append(list.CollapsedRuns, attrs.Prefix)
		} else {
			// Converting attrs to *Object type.
			currObject := storageutil.ObjectAttrsToBucketObject(attrs)
			list.Objects = append(list.Objects, currObject)
		}
	}

	list.ContinuationToken = itr.PageInfo().Token
	listing = &list
	return
}

func (b *bucketHandle) UpdateObject(ctx context.Context, req *gcs.UpdateObjectRequest) (o *gcs.Object, err error) {
	obj := b.bucket.Object(req.Name)

	if req.Generation != 0 {
		obj = obj.Generation(req.Generation)
	}

	if req.MetaGenerationPrecondition != nil {
		obj = obj.If(storage.Conditions{MetagenerationMatch: *req.MetaGenerationPrecondition})
	}

	updateQuery := storage.ObjectAttrsToUpdate{}

	if req.ContentType != nil {
		updateQuery.ContentType = *req.ContentType
	}

	if req.ContentEncoding != nil {
		updateQuery.ContentEncoding = *req.ContentEncoding
	}

	if req.ContentLanguage != nil {
		updateQuery.ContentLanguage = *req.ContentLanguage
	}

	if req.CacheControl != nil {
		updateQuery.CacheControl = *req.CacheControl
	}

	if req.Metadata != nil {
		updateQuery.Metadata = make(map[string]string)
		for key, element := range req.Metadata {
			if element != nil {
				updateQuery.Metadata[key] = *element
			}
		}
	}

	attrs, err := obj.Update(ctx, updateQuery)

	if err == nil {
		// Converting objAttrs to type *Object
		o = storageutil.ObjectAttrsToBucketObject(attrs)
		return
	}

	// If storage object does not exist, httpclient is returning ErrObjectNotExist error instead of googleapi error
	// https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/vendor/cloud.google.com/go/storage/http_client.go#L516
	switch ee := err.(type) {
	case *googleapi.Error:
		if ee.Code == http.StatusPreconditionFailed {
			err = &gcs.PreconditionError{Err: ee}
		}
	default:
		if err == storage.ErrObjectNotExist {
			err = &gcs.NotFoundError{Err: storage.ErrObjectNotExist}
		} else {
			err = fmt.Errorf("Error in updating object: %w", err)
		}
	}

	return
}

func (b *bucketHandle) ComposeObjects(ctx context.Context, req *gcs.ComposeObjectsRequest) (o *gcs.Object, err error) {
	dstObj := b.bucket.Object(req.DstName)

	if req.DstGenerationPrecondition != nil && req.DstMetaGenerationPrecondition != nil {
		dstObj = dstObj.If(storage.Conditions{GenerationMatch: *req.DstGenerationPrecondition, MetagenerationMatch: *req.DstMetaGenerationPrecondition})
	} else if req.DstGenerationPrecondition != nil {
		dstObj = dstObj.If(storage.Conditions{GenerationMatch: *req.DstGenerationPrecondition})
	} else if req.DstMetaGenerationPrecondition != nil {
		dstObj = dstObj.If(storage.Conditions{MetagenerationMatch: *req.DstMetaGenerationPrecondition})
	}

	// Converting the req.Sources list to a list of storage.ObjectHandle as expected by the Go Storage Client.
	var srcObjList []*storage.ObjectHandle
	for _, src := range req.Sources {
		currSrcObj := b.bucket.Object(src.Name)
		// Switching to requested Generation of the object.
		// Zero src generation is the latest generation, we are skipping it because by default it will take the latest one
		if src.Generation != 0 {
			currSrcObj = currSrcObj.Generation(src.Generation)
		}
		srcObjList = append(srcObjList, currSrcObj)
	}

	// Composing Source Objects to Destination Object using Composer created through Go Storage Client.
	attrs, err := dstObj.ComposerFrom(srcObjList...).Run(ctx)
	if err != nil {
		switch ee := err.(type) {
		case *googleapi.Error:
			if ee.Code == http.StatusPreconditionFailed {
				err = &gcs.PreconditionError{Err: ee}
			}
			if ee.Code == http.StatusNotFound {
				err = &gcs.NotFoundError{Err: storage.ErrObjectNotExist}
			}
		default:
			err = fmt.Errorf("Error in composing object: %w", err)
		}
		return
	}

	// Converting attrs to type *Object.
	o = storageutil.ObjectAttrsToBucketObject(attrs)

	return
}
