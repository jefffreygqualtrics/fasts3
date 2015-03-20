package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/TuneOSS/fasts3/awswrapper"
	"github.com/TuneOSS/fasts3/s3wrapper"
	"github.com/alecthomas/kingpin"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/s3"
	"github.com/dustin/go-humanize"
)

const layout = "2006-01-02 15:04:05"

type s3List []string

func (s *s3List) Set(value string) error {
	hasMatch, err := regexp.MatchString("^s3://", value)
	if err != nil {
		return err
	}
	if !hasMatch {
		return fmt.Errorf("%s not a valid S3 uri, Please enter a valid S3 uri. Ex: s3://mary/had/a/little/lamb\n", *lsS3Uri)
	} else {
		*s = append(*s, value)
		return nil
	}
}

func (s *s3List) String() string {
	return ""
}

func (s *s3List) IsCumulative() bool {
	return true
}

func S3List(s kingpin.Settings) (target *[]string) {
	target = new([]string)
	s.SetValue((*s3List)(target))
	return
}

var (
	app = kingpin.New("fasts3", "Multi-threaded s3 utility")

	ls            = app.Command("ls", "List s3 prefixes.")
	lsS3Uri       = ls.Arg("s3uri", "paritial s3 uri to list, ex: s3://mary/had/a/little/lamb/").Required().String()
	lsRecurse     = ls.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	lsSearchDepth = ls.Flag("search-depth", "search depth to search for work.").Default("0").Int()
	humanReadable = ls.Flag("human-readable", "human readable key size.").Short('H').Bool()
	withDate      = ls.Flag("with-date", "include the last modified date.").Short('d').Bool()

	del            = app.Command("del", "Delete s3 keys")
	delPrefixes    = S3List(del.Arg("prefixes", "1 or more partial s3 uris to delete delimited by space"))
	delRecurse     = del.Flag("recursive", "Delete all keys with prefix").Short('r').Bool()
	delSearchDepth = del.Flag("search-depth", "search depth to search for work.").Default("0").Int()

	initApp = app.Command("init", "Initialize .fs3cfg file in home directory")
)

func timeFormat(t time.Time) string {
	return t.Format(layout)
}
func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return
}

func GetS3Service(bucket string) *s3.S3 {
	err, auth := awswrapper.GetAwsAuth()
	if err != nil {
		log.Fatalln(err)
	}
	service := s3.New(auth, "us-east-1", nil)

	loc, err := service.GetBucketLocation(&s3.GetBucketLocationRequest{Bucket: aws.String(bucket)})
	if err != nil {
		log.Fatalln(err)

	}
	if loc.LocationConstraint != nil {
		if *loc.LocationConstraint != "us-east-1" {
			service = s3.New(auth, *loc.LocationConstraint, nil)
		}
	}
	return service
}

// Ls lists directorys or keys under a prefix
func Ls(s3Uri string, searchDepth int, isRecursive, isHumanReadable, includeDate bool) {
	bucket, prefix := parseS3Uri(s3Uri)
	s3Service := GetS3Service(bucket)

	var ch <-chan s3.Object
	if isRecursive {
		ch = s3wrapper.ListRecurse(s3Service, bucket, prefix, searchDepth)
	} else {
		ch = s3wrapper.ListWithCommonPrefixes(s3Service, bucket, prefix)
	}

	for k := range ch {
		if *k.Size < 0 {
			fmt.Printf("%10s s3://%s/%s\n", "DIR", bucket, *k.Key)
		} else {
			var size string
			if isHumanReadable {
				size = fmt.Sprintf("%10s", humanize.Bytes(uint64(*k.Size)))
			} else {
				size = fmt.Sprintf("%10d", *k.Size)
			}
			date := ""
			if includeDate {
				date = " " + timeFormat(k.LastModified)
			}
			fmt.Printf("%s%s s3://%s/%s\n", size, date, bucket, *k.Key)
		}
	}

}

// Del deletes a set of prefixes(s3 keys or partial keys
func Del(prefixes []string, searchDepth int, isRecursive bool) {
	if len(*delPrefixes) == 0 {
		fmt.Printf("No prefixes provided\n Usage: fasts3 del <prefix>")
		return
	}
	keys := make(chan string, len(prefixes)*2+1)
	var s3Service *s3.S3 = nil
	var bucket string = ""
	go func() {
		for _, delPrefix := range prefixes {
			bucket, prefix := parseS3Uri(delPrefix)

			if s3Service == nil {
				s3Service = GetS3Service(bucket)
			}

			keys <- prefix
			if *delRecurse {
				keyExists := s3wrapper.Exists(s3Service, bucket, prefix)

				if keyExists {
					keys <- prefix
				} else if *delRecurse {
					for key := range s3wrapper.ListRecurse(s3Service, bucket, prefix, searchDepth) {
						keys <- *key.Key
					}

				} else {
					fmt.Printf("trying to delete a prefix, please add --recursive or -r to proceed\n")
				}
			}
		}
		close(keys)
	}()

	var wg sync.WaitGroup
	msgs := make(chan string, 1000)
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			batch := make([]string, 0, 100)
			for key := range keys {
				batch = append(batch, key)
				if len(batch) >= 100 {
					err, deleted := s3wrapper.DeleteMulti(s3Service, bucket, batch)
					if err != nil {
						log.Fatalln(err)
					}
					for _, k := range deleted.Deleted {
						msgs <- fmt.Sprintf("File %s Deleted\n", *k.Key)
					}
					batch = batch[:0]
				}
			}

			if len(batch) > 0 {
				err, deleted := s3wrapper.DeleteMulti(s3Service, bucket, batch)
				if err != nil {
					log.Fatalln(err)
				}
				for _, k := range deleted.Deleted {
					msgs <- fmt.Sprintf("File %s Deleted\n", *k.Key)
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(msgs)
	}()
	for msg := range msgs {
		fmt.Print(msg)
	}
}

// initializes configs necessary for fasts3 utility
func Init() error {
	usr, err := user.Current()
	if err != nil {
		return err
	}

	fs3cfg_path := path.Join(usr.HomeDir, ".fs3cfg")
	if _, err := os.Stat(fs3cfg_path); os.IsNotExist(err) {
		cfg := `[default]
access_key=<access_key>
secret_key=<secret_key>`
		ioutil.WriteFile(fs3cfg_path, []byte(cfg), 0644)
		fmt.Printf("created template file %s\n", fs3cfg_path)
	} else {
		fmt.Print(".fs3cfg already exists in home directory")
	}

	return nil
}

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case "ls":
		Ls(*lsS3Uri, *lsSearchDepth, *lsRecurse, *humanReadable, *withDate)
	case "del":
		Del(*delPrefixes, *lsSearchDepth, *delRecurse)
	case "init":
		Init()
	}
}
