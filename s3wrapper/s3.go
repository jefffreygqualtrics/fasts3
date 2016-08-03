package s3wrapper

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/TuneOSS/fasts3/util"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ListOutput struct {
	IsPrefix     bool
	Size         *int64
	Key          *string
	LastModified *time.Time
	Bucket       *string
	FullKey      *string
}

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

func formatS3Uri(bucket string, key string) string {
	return fmt.Sprintf("s3://%s", path.Join(bucket, key))
}

func New(svc *s3.S3) *S3Wrapper {
	// set concurrency limit to GOMAXPROCS if set, else default to 4x CPUs
	var ch chan struct{}
	if os.Getenv("GOMAXPROCS") != "" {
		ch = make(chan struct{}, runtime.GOMAXPROCS(0))
	} else {
		ch = make(chan struct{}, 4*runtime.NumCPU())
	}

	return &S3Wrapper{
		svc:                  svc,
		concurrencySemaphore: ch,
	}
}

func (w *S3Wrapper) ListAll(s3Uris []string, recursive bool, delimiter string, keyRegex *string) chan *ListOutput {
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

func (w *S3Wrapper) List(s3Uri string, recursive bool, delimiter string, keyRegex *string) chan *ListOutput {
	bucket, prefix := parseS3Uri(s3Uri)
	if recursive {
		delimiter = ""
	}
	var keyRegexFilter *regexp.Regexp = nil
	if keyRegex != nil {
		keyRegexFilter = regexp.MustCompile(*keyRegex)
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
					formattedKey := formatS3Uri(bucket, *prefix.Prefix)
					ch <- &ListOutput{
						IsPrefix:     true,
						Key:          prefix.Prefix,
						FullKey:      &formattedKey,
						LastModified: nil,
						Size:         nil,
						Bucket:       &bucket,
					}
				}
			}

			for _, key := range page.Contents {
				formattedKey := formatS3Uri(bucket, *key.Key)
				if keyRegexFilter != nil && !keyRegexFilter.MatchString(formattedKey) {
					continue
				}
				ch <- &ListOutput{
					IsPrefix:     false,
					Key:          key.Key,
					FullKey:      &formattedKey,
					LastModified: key.LastModified,
					Size:         key.Size,
					Bucket:       &bucket,
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
func (w *S3Wrapper) Stream(keys chan *ListOutput, includeKeyName bool) chan string {
	lines := make(chan string, 10000)
	var wg sync.WaitGroup
	go func() {
		for key := range keys {
			wg.Add(1)
			go func(key *ListOutput) {
				defer wg.Done()
				w.concurrencySemaphore <- struct{}{}
				defer func() { <-w.concurrencySemaphore }()

				reader, err := w.GetReader(*key.Bucket, *key.Key)
				if err != nil {
					panic(err)
				}
				ext_reader, err := util.GetReaderByExt(reader, *key.Key)
				if err != nil {
					panic(err)
				}

				for {
					line, _, err := ext_reader.ReadLine()
					if err != nil {
						if err.Error() == "EOF" {
							break
						} else {
							log.Fatalln(err)
						}
					}
					if includeKeyName {
						lines <- fmt.Sprintf("[%s] %s", *key.FullKey, string(line))
					} else {
						lines <- fmt.Sprintf("%s", string(line))
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

func (w *S3Wrapper) GetAll(keys chan *ListOutput) chan *ListOutput {
	listOut := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for key := range keys {
		wg.Add(1)
		go func(k *ListOutput) {
			defer wg.Done()
			w.concurrencySemaphore <- struct{}{}
			defer func() { <-w.concurrencySemaphore }()

			if !k.IsPrefix {
				// TODO: this assumes '/' as a delimiter
				parts := strings.Split(*k.Key, "/")
				dir := strings.Join(parts[0:len(parts)-1], "/")
				util.CreatePathIfNotExists(dir)
				reader, err := w.GetReader(*k.Bucket, *k.Key)
				defer reader.Close()
				if err != nil {
					panic(err)
				}
				outFile, err := os.Create(*k.Key)
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

	go func() {
		wg.Wait()
		close(listOut)
	}()

	return listOut
}

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
				kBucket, kPrefix := parseS3Uri(*k.FullKey)
				sourcePath := "/" + path.Join(kBucket, kPrefix)

				// trim common path prefixes from k.Key and sourcePrefix
				trimDest := strings.Split(*k.Key, delimiter)
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
					k.Key = &fullDest
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
