package s3wrapper

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// ListOutput represents the pruned and
// normalized result of a list call to S3,
// this is meant to cut down on memory and
// overhead being used in the channels
type ListOutput struct {
	IsPrefix     bool
	Size         int64
	Key          string
	LastModified time.Time
	Bucket       string
	FullKey      string
}

// S3Wrapper is a wrapper for the S3
// library which aims to make some of
// it's functions faster
type S3Wrapper struct {
	concurrencySemaphore chan struct{}
	svc                  *s3.S3
}

// parseS3Uri parses a s3 uri into its bucket and prefix
func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return bucket, prefix
}

// FormatS3Uri takes a bucket and a prefix and turns it into
// a S3 URI
func FormatS3Uri(bucket string, key string) string {
	return fmt.Sprintf("s3://%s", path.Join(bucket, key))
}

// New creates a new S3Wrapper
func New(svc *s3.S3, maxParallel int) *S3Wrapper {
	return &S3Wrapper{
		svc:                  svc,
		concurrencySemaphore: make(chan struct{}, maxParallel),
	}
}

func (w *S3Wrapper) WithRegionFrom(uri string) (*S3Wrapper, error) {
	bucket, _ := parseS3Uri(uri)
	region, err := s3manager.GetBucketRegionWithClient(context.Background(), w.svc, bucket)
	if err != nil {
		log.Printf("WARN: unable to autodetect region, falling back to default. Cause: '%s'\n", err)
		return w, nil
	}
	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		return nil, err
	}
	w.svc = s3.New(sess, aws.NewConfig().WithRegion(region))
	return w, nil
}

// WithMaxConcurrency sets the maximum concurrency for the S3 operations
func (w *S3Wrapper) WithMaxConcurrency(maxConcurrency int) *S3Wrapper {
	w.concurrencySemaphore = make(chan struct{}, maxConcurrency)
	return w
}

// ListAll is a convienience function for listing and collating all the results for multiple S3 URIs
func (w *S3Wrapper) ListAll(s3Uris []string, recursive bool, delimiter string, keyRegex string) chan *ListOutput {
	ch := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for _, s3Uri := range s3Uris {
		wg.Add(1)
		go func(s3Uri string) {
			defer wg.Done()
			for itm := range w.List(s3Uri, recursive, delimiter, keyRegex) {
				ch <- itm
			}
		}(s3Uri)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}

// List is a wrapping function to parallelize listings and normalize the results from the API
func (w *S3Wrapper) List(s3Uri string, recursive bool, delimiter string, keyRegex string) chan *ListOutput {
	bucket, prefix := parseS3Uri(s3Uri)
	if recursive {
		delimiter = ""
	}
	var keyRegexFilter *regexp.Regexp
	if keyRegex != "" {
		keyRegexFilter = regexp.MustCompile(keyRegex)
	}

	params := &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket), // Required
		Delimiter:    aws.String(delimiter),
		EncodingType: aws.String(s3.EncodingTypeUrl),
		FetchOwner:   aws.Bool(false),
		MaxKeys:      aws.Int64(1000),
		Prefix:       aws.String(prefix),
	}

	ch := make(chan *ListOutput, 10000)
	go func() {
		defer close(ch)
		w.concurrencySemaphore <- struct{}{}
		defer func() { <-w.concurrencySemaphore }()

		err := w.svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, prefix := range page.CommonPrefixes {
				if *prefix.Prefix != delimiter {
					escapedPrefix, err := url.QueryUnescape(*prefix.Prefix)
					if err != nil {
						escapedPrefix = *prefix.Prefix
					}
					formattedKey := FormatS3Uri(bucket, escapedPrefix)
					ch <- &ListOutput{
						IsPrefix:     true,
						Key:          escapedPrefix,
						FullKey:      formattedKey,
						LastModified: time.Time{},
						Size:         0,
						Bucket:       bucket,
					}
				}
			}

			for _, key := range page.Contents {
				escapedKey, err := url.QueryUnescape(*key.Key)
				if err != nil {
					escapedKey = *key.Key
				}
				formattedKey := FormatS3Uri(bucket, escapedKey)
				if keyRegexFilter != nil && !keyRegexFilter.MatchString(formattedKey) {
					continue
				}
				ch <- &ListOutput{
					IsPrefix:     false,
					Key:          escapedKey,
					FullKey:      formattedKey,
					LastModified: *key.LastModified,
					Size:         *key.Size,
					Bucket:       bucket,
				}
			}
			return true
		})
		if err != nil {
			panic(err)
		}
	}()

	return ch
}

// GetReader retrieves an appropriate reader for the given bucket and key
func (w *S3Wrapper) GetReader(bucket string, key string) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := w.svc.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Stream provides a channel with data from the keys
func (w *S3Wrapper) Stream(keys chan *ListOutput, includeKeyName bool, raw bool) chan string {
	lines := make(chan string, 10000)
	var wg sync.WaitGroup
	go func() {
		for key := range keys {
			wg.Add(1)
			go func(key *ListOutput) {
				defer wg.Done()
				w.concurrencySemaphore <- struct{}{}
				defer func() { <-w.concurrencySemaphore }()

				reader, err := w.GetReader(key.Bucket, key.Key)
				if err != nil {
					panic(err)
				}
				defer reader.Close()
				if !raw {
					extReader, err := getReaderByExt(reader, key.Key)
					if err != nil {
						panic(err)
					}
					bufExtReader := bufio.NewReader(extReader)

					for {
						line, err := bufExtReader.ReadBytes('\n')

						if err != nil && err.Error() != "EOF" {
							log.Fatalln(err)
						}

						if includeKeyName {
							lines <- fmt.Sprintf("[%s] %s", key.FullKey, string(line))
						} else {
							lines <- string(line)
						}
						if err != nil {
							break
						}
					}
				} else {
					buf := make([]byte, 64)
					for {
						numBytes, err := reader.Read(buf)
						if err != nil && err.Error() != "EOF" {
							log.Fatalln(err)
						}

						if includeKeyName {
							lines <- fmt.Sprintf("[%s] %s", key.FullKey, string(buf[0:numBytes]))
						} else {
							lines <- string(buf[0:numBytes])
						}

						if err != nil {
							break
						}
					}
				}
			}(key)
		}
		go func() {
			wg.Wait()
			close(lines)
		}()
	}()

	return lines
}

// GetAll retrieves all keys to the local filesystem, it repurposes ListOutput as it's
// output which contains the local paths to the keys
func (w *S3Wrapper) GetAll(keys chan *ListOutput, skipExisting bool) chan *ListOutput {
	listOut := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for key := range keys {
		if _, err := os.Stat(key.Key); skipExisting == false || os.IsNotExist(err) {
			wg.Add(1)
			go func(k *ListOutput) {
				defer wg.Done()
				w.concurrencySemaphore <- struct{}{}
				defer func() { <-w.concurrencySemaphore }()

				if !k.IsPrefix {
					// TODO: this assumes '/' as a delimiter
					parts := strings.Split(k.Key, "/")
					dir := strings.Join(parts[0:len(parts)-1], "/")
					createPathIfNotExists(dir)
					reader, err := w.GetReader(k.Bucket, k.Key)
					if err != nil {
						panic(err)
					}
					defer reader.Close()
					outFile, err := os.Create(k.Key)
					if err != nil {
						panic(err)
					}
					defer outFile.Close()
					_, err = io.Copy(outFile, reader)
					if err != nil {
						panic(err)
					}
					listOut <- k
				}
			}(key)
		}
	}

	go func() {
		wg.Wait()
		close(listOut)
	}()

	return listOut
}

// CopyAll copies keys to the dest, source defines what the base prefix is
func (w *S3Wrapper) CopyAll(keys chan *ListOutput, source, dest string, delimiter string, recurse, flat bool) chan *ListOutput {
	_, sourcePrefix := parseS3Uri(source)
	destBucket, destPrefix := parseS3Uri(dest)

	listOut := make(chan *ListOutput, 1e4)
	var wg sync.WaitGroup
	for key := range keys {
		wg.Add(1)
		go func(k *ListOutput) {
			defer wg.Done()
			w.concurrencySemaphore <- struct{}{}
			defer func() { <-w.concurrencySemaphore }()

			if !k.IsPrefix {
				keyBucket, keyPrefix := parseS3Uri(k.FullKey)
				sourcePath := "/" + path.Join(keyBucket, keyPrefix)

				// trim common path prefixes from k.Key and sourcePrefix
				trimDest := strings.Split(k.Key, delimiter)
				if flat {
					trimDest = trimDest[len(trimDest)-1:]
				} else if recurse {
					trimSource := strings.Split(sourcePrefix, delimiter)
					for len(trimDest) > 1 && len(trimSource) > 1 {
						if trimDest[0] != trimSource[0] {
							break
						}
						trimDest = trimDest[1:]
						trimSource = trimSource[1:]
					}
				}
				fullDest := destPrefix + strings.Join(trimDest, delimiter)

				_, err := w.svc.CopyObject(&s3.CopyObjectInput{
					Bucket:     &destBucket,
					CopySource: &sourcePath,
					Key:        &fullDest,
				})
				if err != nil {
					fmt.Println("error:", err)
				} else {
					k.Key = fullDest
					listOut <- k
				}
			}
		}(key)
	}

	go func() {
		wg.Wait()
		close(listOut)
	}()

	return listOut
}

// ListBuckets returns a list of bucket names and does a prefix
// filter based on s3Uri (of the form s3://<bucket-prefix>)
func (w *S3Wrapper) ListBuckets(s3Uri string) ([]string, error) {

	bucketPrefix, _ := parseS3Uri(s3Uri)
	results, err := w.svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	buckets := make([]string, 0, len(results.Buckets))
	for _, bucket := range results.Buckets {
		if *bucket.Name != "" && !strings.HasPrefix(*bucket.Name, bucketPrefix) {
			continue
		}
		buckets = append(buckets, *bucket.Name)
	}
	return buckets, nil
}

const maxKeysPerDeleteObjectsRequest = 1000

// DeleteObjects deletes all keys in the given keys channel
func (w *S3Wrapper) DeleteObjects(keys chan *ListOutput) chan *ListOutput {
	listOut := make(chan *ListOutput, 1e4)
	var wg sync.WaitGroup

	for i := 0; i < cap(w.concurrencySemaphore); i++ {
		wg.Add(1)
		go func() {
			w.concurrencySemaphore <- struct{}{}
			defer func() { <-w.concurrencySemaphore }()
			defer wg.Done()
			objects := make([]*s3.ObjectIdentifier, 0, maxKeysPerDeleteObjectsRequest)
			listOutCache := make([]*ListOutput, 0, maxKeysPerDeleteObjectsRequest)
			params := &s3.DeleteObjectsInput{
				Bucket: aws.String(""),
				Delete: &s3.Delete{},
			}
			for item := range keys {
				if item.IsPrefix {
					continue
				}

				if *params.Bucket == "" {
					params.Bucket = aws.String(item.Bucket)
				}
				// only maxKeysPerDeleteObjectsRequest objects can fit in
				// one DeleteObjects request also if the bucket changes we cannot
				// put it in the same request so we flush and start a new one
				if len(objects) >= maxKeysPerDeleteObjectsRequest || *params.Bucket != item.Bucket {
					// flush
					params.Delete = &s3.Delete{
						Objects: objects,
					}
					_, err := w.svc.DeleteObjects(params)
					if err != nil {
						panic(err)
					}

					// write the keys deleted to the results channel
					for _, cacheItem := range listOutCache {
						listOut <- cacheItem
					}

					// reset
					listOutCache = make([]*ListOutput, 0, maxKeysPerDeleteObjectsRequest)
					params.Bucket = aws.String(item.Bucket)
					objects = make([]*s3.ObjectIdentifier, 0, maxKeysPerDeleteObjectsRequest)
				}
				objects = append(objects, &s3.ObjectIdentifier{
					Key: aws.String(item.Key),
				})
				listOutCache = append(listOutCache, item)
			}
			if len(objects) > 0 {
				// flush again for any remaining keys
				params.Delete = &s3.Delete{
					Objects: objects,
				}
				_, err := w.svc.DeleteObjects(params)
				if err != nil {
					panic(err)
				}

				for _, cacheItem := range listOutCache {
					listOut <- cacheItem
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(listOut)
	}()

	return listOut
}

// getReaderByExt is a factory for reader based on the extension of the key
func getReaderByExt(reader io.ReadCloser, key string) (io.ReadCloser, error) {
	ext := path.Ext(key)
	if ext == ".gz" || ext == ".gzip" {
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return reader, nil
		}
		return gzReader, nil
	}

	return reader, nil
}

// createPathIfNotExists takes a path and creates
// it if it doesn't exist
func createPathIfNotExists(path string) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return nil
	}
	return os.MkdirAll(path, 0755)
}
