package s3wrapper

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
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
	fileDescriptorSemaphore chan bool
	svc                     *s3.S3
}

// parseS3Uri parses a s3 uri into it's bucket and prefix
func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return bucket, prefix
}

func formatS3Uri(bucket string, key string) string {
	return fmt.Sprintf("s3://%s", path.Join(bucket, key))
}

func New(svc *s3.S3) (*S3Wrapper, error) {
	num, err := util.GetNumFileDescriptors()
	ch := make(chan bool, num/2)
	s3Wrapper := S3Wrapper{svc: svc, fileDescriptorSemaphore: ch}
	return &s3Wrapper, err
}
func (w *S3Wrapper) ListAll(s3Uris []string, recursive bool, delimiter string, keyRegex *string) chan *ListOutput {
	ch := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for _, s3Uri := range s3Uris {
		wg.Add(1)
		go func(s3Uri string) {
			for itm := range w.List(s3Uri, recursive, delimiter, keyRegex) {
				ch <- itm
			}
			wg.Done()
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
		w.fileDescriptorSemaphore <- true
		err := w.svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, prefix := range page.CommonPrefixes {
				if *prefix.Prefix != "/" {
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
		<-w.fileDescriptorSemaphore
		close(ch)
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
	for key := range keys {
		wg.Add(1)
		go func(key *ListOutput) {
			w.fileDescriptorSemaphore <- true
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
				lines <- fmt.Sprintf("[%s] %s", *key.FullKey, string(line))
			}
			<-w.fileDescriptorSemaphore
			wg.Done()
		}(key)
	}
	go func() {
		wg.Wait()
		close(lines)
	}()

	return lines

}

func (w *S3Wrapper) GetAll(keys chan *ListOutput) chan *ListOutput {
	listOut := make(chan *ListOutput, 10000)
	var wg sync.WaitGroup
	for key := range keys {
		wg.Add(1)
		go func(k *ListOutput) {
			w.fileDescriptorSemaphore <- true
			if !k.IsPrefix {
				parts := strings.Split(*k.Key, "/")
				dir := strings.Join(parts[0:len(parts)-1], "/")
				util.CreatePathIfNotExists(dir)
				reader, err := w.GetReader(*k.Bucket, *k.Key)
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
			<-w.fileDescriptorSemaphore
			wg.Done()
		}(key)
	}

	go func() {
		wg.Wait()
		close(listOut)
	}()

	return listOut
}
