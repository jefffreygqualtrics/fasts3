package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/TuneOSS/fasts3/s3wrapper"
	"github.com/TuneOSS/fasts3/util"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("fasts3", "A faster s3 utility")

	ls              = app.Command("ls", "List s3 prefixes.")
	lsS3Uris        = util.S3List(ls.Arg("s3Uris", "list of s3 uris"))
	lsRecurse       = ls.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	lsWithDate      = ls.Flag("with-date", "include the last modified date.").Short('d').Bool()
	lsDelimiter     = ls.Flag("delimiter", "delimiter to use while listing").Default("/").String()
	lsHumanReadable = ls.Flag("human-readable", "delimiter to use while listing").Short('H').Bool()
	lsSearchDepth   = ls.Flag("search-depth", "Dictates how many prefix groups to walk down").Default("0").Int()

	stream               = app.Command("stream", "Stream s3 files to stdout")
	streamS3Uris         = util.S3List(stream.Arg("s3Uris", "list of s3 uris"))
	streamKeyRegex       = stream.Flag("key-regex", "regex filter for keys").Default("").String()
	streamDelimiter      = stream.Flag("delimiter", "delimiter to use while listing").Default("/").String()
	streamIncludeKeyName = stream.Flag("include-key-name", "regex filter for keys").Bool()
	streamSearchDepth    = stream.Flag("search-depth", "Dictates how many prefix groups to walk down").Default("0").Int()

	get            = app.Command("get", "Fetch files from s3")
	getS3Uris      = util.S3List(get.Arg("s3Uris", "list of s3 uris"))
	getRecurse     = get.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	getDelimiter   = get.Flag("delimiter", "delimiter to use while listing").Default("/").String()
	getSearchDepth = get.Flag("search-depth", "Dictates how many prefix groups to walk down").Default("0").Int()
	getKeyRegex    = get.Flag("key-regex", "regex filter for keys").Default("").String()
)

func Ls(svc *s3.S3, s3Uris []string, recursive bool, delimiter string, searchDepth int, keyRegex *string) (chan *s3wrapper.ListOutput, error) {
	wrap, err := s3wrapper.New(svc)
	if err != nil {
		return nil, err
	}
	outChan := make(chan *s3wrapper.ListOutput, 10000)
	for i := 0; i < searchDepth; i++ {
		newS3Uris := make([]string, 0)
		for itm := range wrap.ListAll(s3Uris, false, delimiter, keyRegex) {
			if itm.IsPrefix {
				newS3Uris = append(newS3Uris, strings.TrimRight(*itm.FullKey, "/")+"/")
			} else {
				outChan <- itm
			}
		}
		s3Uris = newS3Uris
	}

	go func() {
		for itm := range wrap.ListAll(s3Uris, recursive, delimiter, keyRegex) {
			outChan <- itm
		}
		close(outChan)
	}()

	return outChan, nil
}

func PrintLs(listChan chan *s3wrapper.ListOutput, humanReadable bool, includeDates bool) {
	for listOutput := range listChan {

		if listOutput.IsPrefix {
			fmt.Printf("%10s %s\n", "DIR", *listOutput.FullKey)
		} else {
			var size string
			if humanReadable {
				size = fmt.Sprintf("%10s", humanize.Bytes(uint64(*listOutput.Size)))
			} else {
				size = fmt.Sprintf("%10d", *listOutput.Size)
			}
			date := ""
			if includeDates {
				date = " " + (*listOutput.LastModified).Format("2006-01-02T15:04:05")
			}
			fmt.Printf("%s%s %s\n", size, date, *listOutput.FullKey)
		}
	}
}

func Stream(svc *s3.S3, s3Uris []string, delimiter string, searchDepth int, includeKeyName bool, keyRegex *string) error {

	listCh, err := Ls(svc, s3Uris, true, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}
	wrap, err := s3wrapper.New(svc)
	if err != nil {
		return err
	}

	lines := wrap.Stream(listCh, includeKeyName)
	for line := range lines {
		fmt.Println(line)
	}

	return nil
}

func Get(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex *string) error {
	listCh, err := Ls(svc, s3Uris, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap, err := s3wrapper.New(svc)
	if err != nil {
		return err
	}

	downloadedFiles := wrap.GetAll(listCh)
	for file := range downloadedFiles {
		fmt.Printf("Downloaded %s -> %s\n", *file.FullKey, *file.Key)
	}

	return nil
}

func main() {
	app.Version("1.1.0")
	aws_session := session.New()
	svc := s3.New(aws_session, aws.NewConfig().WithRegion("us-east-1"))

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	// Register user
	case ls.FullCommand():
		listCh, err := Ls(svc, *lsS3Uris, *lsRecurse, *lsDelimiter, *lsSearchDepth, nil)
		if err != nil {
			panic(err)
		}
		PrintLs(listCh, *lsHumanReadable, *lsWithDate)

	case stream.FullCommand():
		err := Stream(svc, *streamS3Uris, *streamDelimiter, *streamSearchDepth, *streamIncludeKeyName, streamKeyRegex)
		if err != nil {
			panic(err)
		}
	case get.FullCommand():
		Get(svc, *getS3Uris, *getRecurse, *getDelimiter, *getSearchDepth, getKeyRegex)
	}
}
