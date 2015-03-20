package s3wrapper

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/s3"
)

const defaultDelimiter string = "/"

const numListRoutines int = 30

const maxRetries int = 5

type listWork struct {
	prefix    string
	delimiter string
}

// StripS3Path gets the key part from an s3 uri. The key part is everything after
// the header (s3://<bucket>)
func StripS3Path(key string) string {
	key = strings.TrimRight(key, "\n")
	if strings.Index(key, "s3://") < 0 {
		return key // IF s3:// is not located, assume the file is already stripped
	}

	// Take the first element after splitting from tab (Should be 's3://.../.../.../...')
	metaFilePath := strings.Split(key, "\t")[0]
	// Clear everything off the line before 's3://' to allow taking s3cmd ls output as input
	s3Path := metaFilePath[strings.Index(metaFilePath, "s3://"):]
	// The bucket is the second element if you split the string 's3://<bucket>/' by '/'
	bucket := strings.Split(s3Path, "/")[2]
	header := "s3://" + bucket
	return metaFilePath[strings.Index(metaFilePath, header)+len(header):]
}

// getListWork generates a list of prefixes based on prefix by searching down the searchDepth
// using DELIMITER as a delimiter
func getListWork(s3Service *s3.S3, bucket string, prefix string, searchDepth int) []listWork {
	currentPrefixes := []listWork{listWork{prefix, ""}}
	results := []listWork{}
	for i := 0; i < searchDepth; i++ {
		newPrefixes := []listWork{}
		for _, pfx := range currentPrefixes {
			for res := range List(s3Service, bucket, pfx.prefix, defaultDelimiter) {
				if len(res.CommonPrefixes) != 0 {
					for _, newPfx := range res.CommonPrefixes {
						newPrefixes = append(newPrefixes, listWork{*newPfx.Prefix, ""})
					}
					// catches the case where keys and common prefixes live in the same place
					if len(res.Contents) > 0 {
						results = append(results, listWork{pfx.prefix, "/"})
					}
				} else {
					results = append(results, listWork{pfx.prefix, ""})
				}
			}
		}
		currentPrefixes = newPrefixes
	}
	for _, pfx := range currentPrefixes {
		results = append(results, pfx)
	}
	return results
}

func Exists(s3Service *s3.S3, bucket, key string) bool {
	c := 0
	for resp := range List(s3Service, bucket, key, "/") {
		c += len(resp.Contents)
	}
	return c > 0
}

// List function with retry and support for listing all keys in a prefix
func List(s3Service *s3.S3, bucket string, prefix string, delimiter string) <-chan *s3.ListObjectsOutput {
	ch := make(chan *s3.ListObjectsOutput, 100)
	go func(pfix string, del string) {
		listReq := s3.ListObjectsRequest{
			Bucket:    aws.String(bucket),
			Delimiter: aws.String(delimiter),
			MaxKeys:   aws.Integer(1000),
			Marker:    nil,
			Prefix:    aws.String(pfix)}
		defer close(ch)
		isTruncated := true
		for isTruncated {
			attempts := 0
			for {
				attempts++
				res, err := s3Service.ListObjects(&listReq)
				if err != nil {
					if err.Error() == "runtime error: index out of range" {
						break
					}
					if attempts >= maxRetries {
						log.Panic(err)
					}

					time.Sleep(time.Second * 3)
				} else {
					ch <- res
					listReq.Marker = res.NextMarker
					isTruncated = *res.IsTruncated
					break
				}
			}
		}
	}(prefix, delimiter)
	return ch
}

func toDeleteStruct(keys []string) *s3.Delete {
	objs := make([]s3.ObjectIdentifier, 0)
	for _, key := range keys {
		if key != "" {
			objs = append(objs, s3.ObjectIdentifier{Key: aws.String(strings.TrimSpace(key))})
		}
	}
	return &s3.Delete{Objects: objs, Quiet: aws.Boolean(false)}
}

// DeleteMulti  deletes multiple keys
func DeleteMulti(s3Service *s3.S3, bucket string, keys []string) (error, *s3.DeleteObjectsOutput) {
	attempts := 0
	var resp *s3.DeleteObjectsOutput
	for {
		attempts++
		resp, err := s3Service.DeleteObjects(&s3.DeleteObjectsRequest{Bucket: aws.String(bucket), Delete: toDeleteStruct(keys)})
		if err != nil {
			if attempts >= maxRetries {
				return err, resp
			}

			time.Sleep(time.Second * 3)
		} else {
			break
		}
	}
	return nil, resp
}

// lists a prefix and includes common prefixes
func ListWithCommonPrefixes(s3Service *s3.S3, bucket string, prefix string) <-chan s3.Object {
	ch := make(chan s3.Object, 1000)
	go func() {
		defer close(ch)
		for listResp := range List(s3Service, bucket, prefix, defaultDelimiter) {
			for _, prefix := range listResp.CommonPrefixes {
				ch <- s3.Object{Key: prefix.Prefix, Size: aws.Long(-1), Owner: &s3.Owner{}}
			}
			for _, key := range listResp.Contents {
				ch <- key
			}
		}
	}()
	return ch
}

// min function for integers
func intMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func intMax(x, y int) int {
	if x < y {
		return y
	}
	return x
}

// partition partitions list into list of lists where len(lists) <= partitions
func partition(list []listWork, partitionSize int) [][]listWork {
	partitions := [][]listWork{}
	step := intMax(len(list)/partitionSize, 1)
	for i := 0; i < len(list); i += step {
		outerBound := intMin(len(list), i+step)
		partitions = append(partitions, list[i:outerBound])
	}
	return partitions
}

// listRecurse lists prefix in parallel using searchDepth to search for routine's work
func ListRecurse(s3Service *s3.S3, bucket string, prefix string, searchDepth int) <-chan s3.Object {
	ch := make(chan s3.Object, 2000)
	var wg sync.WaitGroup

	workPartitions := partition(getListWork(s3Service, bucket, prefix, searchDepth), numListRoutines)
	for _, partition := range workPartitions {
		wg.Add(1)
		go func(work []listWork) {
			defer wg.Done()
			for _, workItem := range work {
				for res := range List(s3Service, bucket, workItem.prefix, workItem.delimiter) {
					for _, c := range res.Contents {
						ch <- c
					}
				}
			}
		}(partition)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}
