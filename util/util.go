package util

import (
	"log"
	"strings"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/TuneOSS/fasts3/awswrapper"
)

// parseS3Uri parses a s3 uri into it's bucket and prefix
func ParseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return
}

// GetBucket builds a s3 connection retrieving the bucket
func GetBucket(bucket string) *s3.Bucket {
	auth, err := awswrapper.GetAwsAuth()
	if err != nil {
		log.Fatalln(err)
	}
	b := s3.New(auth, aws.USEast).Bucket(bucket)
	loc, err := b.Location()
	if err != nil {
		log.Fatalln(err)

	}
	if aws.GetRegion(loc) != aws.USEast {
		b = s3.New(auth, aws.GetRegion(loc)).Bucket(bucket)
	}
	return b
}
