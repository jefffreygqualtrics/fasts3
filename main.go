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
	humanize "github.com/dustin/go-humanize"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("fasts3", "A faster s3 utility")

	ls              = app.Command("ls", "List s3 prefixes.")
	lsS3Uris        = util.S3List(ls.Arg("s3Uris", "list of s3 URIs"))
	lsRecurse       = ls.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	lsWithDate      = ls.Flag("with-date", "Include the last modified date.").Short('d').Bool()
	lsDelimiter     = ls.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	lsHumanReadable = ls.Flag("human-readable", "Output human-readable object sizes.").Short('H').Bool()
	lsSearchDepth   = ls.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()

	stream               = app.Command("stream", "Stream s3 files to stdout")
	streamS3Uris         = util.S3List(stream.Arg("s3Uris", "list of s3 URIs"))
	streamKeyRegex       = stream.Flag("key-regex", "Regex filter for keys").Default("").String()
	streamDelimiter      = stream.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	streamIncludeKeyName = stream.Flag("include-key-name", "Regex filter for keys.").Bool()
	streamSearchDepth    = stream.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()

	get            = app.Command("get", "Fetch files from s3")
	getS3Uris      = util.S3List(get.Arg("s3Uris", "list of s3 URIs"))
	getRecurse     = get.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	getDelimiter   = get.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	getSearchDepth = get.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	getKeyRegex    = get.Flag("key-regex", "Regex filter for keys.").Default("").String()

	cp            = app.Command("cp", "Copy files within s3")
	cpS3Uris      = util.S3List(cp.Arg("s3Uris", "list of s3 URIs"))
	cpRecurse     = cp.Flag("recursive", "Copy all keys for this prefix.").Short('r').Bool()
	cpFlat        = cp.Flag("flat", "Copy all source files into a flat destination folder (vs. corresponding subfolders)").Short('f').Bool()
	cpDelimiter   = cp.Flag("delimiter", "Delimiter to use while copying.").Default("/").String()
	cpSearchDepth = cp.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	cpKeyRegex    = cp.Flag("key-regex", "Regex filter for keys.").Default("").String()
)

func Ls(svc *s3.S3, s3Uris []string, recursive bool, delimiter string, searchDepth int, keyRegex *string) (chan *s3wrapper.ListOutput, error) {
	wrap := s3wrapper.New(svc)
	outChan := make(chan *s3wrapper.ListOutput, 10000)
	go func() {
		defer close(outChan)

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

		for itm := range wrap.ListAll(s3Uris, recursive, delimiter, keyRegex) {
			outChan <- itm
		}
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
	wrap := s3wrapper.New(svc)

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

	wrap := s3wrapper.New(svc)

	downloadedFiles := wrap.GetAll(listCh)
	for file := range downloadedFiles {
		fmt.Printf("Downloaded %s -> %s\n", *file.FullKey, *file.Key)
	}

	return nil
}

func Cp(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex *string, flat bool) error {
	if len(s3Uris) != 2 {
		fmt.Println("fasts3: error: must include one source and one destination URI")
		os.Exit(1)
	}

	listCh, err := Ls(svc, []string{s3Uris[0]}, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc)

	copiedFiles := wrap.CopyAll(listCh, s3Uris[0], s3Uris[1], delimiter, recurse, flat)
	for file := range copiedFiles {
		fmt.Printf("Copied %s -> %s%s%s\n", *file.FullKey, strings.TrimRight(s3Uris[1], delimiter), delimiter, *file.Key)
	}

	return nil
}

func main() {
	app.Version("1.2.0")
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
	case cp.FullCommand():
		if err := Cp(svc, *cpS3Uris, *cpRecurse, *cpDelimiter, *cpSearchDepth, cpKeyRegex, *cpFlat); err != nil {
			panic(err)
		}
	}
}
