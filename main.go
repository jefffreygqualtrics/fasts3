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

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/TuneOSS/fasts3/awswrapper"
	"github.com/TuneOSS/fasts3/s3wrapper"
	"github.com/alecthomas/kingpin"
	"github.com/dustin/go-humanize"
)

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
	delPrefixes    = S3List(del.Arg("prefixes", "partial s3 uri to del all keys under it"))
	delRecurse     = del.Flag("recursive", "Delete all keys with prefix").Short('r').Bool()
	delSearchDepth = del.Flag("search-depth", "search depth to search for work.").Default("0").Int()

	initApp = app.Command("init", "Initialize .fs3cfg file in home directory")
)

func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return
}

func GetBucket(bucket string) *s3.Bucket {
	err, auth := awswrapper.GetAwsAuth()
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

// Ls lists directorys or keys under a prefix
func Ls(s3Uri string) {
	bucket, prefix := parseS3Uri(s3Uri)
	b := GetBucket(bucket)

	var ch <-chan s3.Key
	if *lsRecurse {
		ch = s3wrapper.ListRecurse(b, prefix, *lsSearchDepth)
	} else {
		ch = s3wrapper.ListWithCommonPrefixes(b, prefix)
	}

	for k := range ch {
		if k.Size < 0 {
			fmt.Printf("%10s s3://%s/%s\n", "DIR", bucket, k.Key)
		} else {
			var size string
			if *humanReadable {
				size = fmt.Sprintf("%10s", humanize.Bytes(uint64(k.Size)))
			} else {
				size = fmt.Sprintf("%10d", k.Size)
			}
			date := ""
			if *withDate {
				date = " " + k.LastModified
			}
			fmt.Printf("%s%s s3://%s/%s\n", size, date, bucket, k.Key)
		}
	}

}

// Del deletes a set of prefixes(s3 keys or partial keys
func Del(prefixes []string) {
	if len(*delPrefixes) == 0 {
		fmt.Printf("No prefixes provided\n Usage: fasts3 del <prefix>")
		return
	}
	keys := make(chan string, len(prefixes)*2+1)
	var b *s3.Bucket = nil
	go func() {
		for _, delPrefix := range prefixes {
			bucket, prefix := parseS3Uri(delPrefix)

			if b == nil {
				b = GetBucket(bucket)
			}

			keys <- prefix
			if *delRecurse {
				keyExists, err := b.Exists(prefix)
				if err != nil {
					log.Fatalln(err)
				}

				if keyExists {
					keys <- prefix
				} else if *delRecurse {
					for key := range s3wrapper.ListRecurse(b, prefix, *delSearchDepth) {
						keys <- key.Key
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
					err := s3wrapper.DeleteMulti(b, batch)
					if err != nil {
						log.Fatalln(err)
					}
					for _, k := range batch {
						msgs <- fmt.Sprintf("File %s Deleted\n", k)
					}
					batch = batch[:0]
				}
			}

			if len(batch) > 0 {
				err := s3wrapper.DeleteMulti(b, batch)
				if err != nil {
					log.Fatalln(err)
				}
				for _, k := range batch {
					msgs <- fmt.Sprintf("File %s Deleted\n", k)
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
		Ls(*lsS3Uri)
	case "del":
		Del(*delPrefixes)
	case "init":
		Init()
	}
}
