package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	humanize "github.com/dustin/go-humanize"
	"github.com/tuneinc/fasts3/s3wrapper"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app         = kingpin.New("fasts3", "A faster s3 utility")
	maxParallel = app.Flag("parallel", "Maximum number of calls to make to S3 simultaneously").Short('p').Default("10").Int()

	ls              = app.Command("ls", "List S3 prefixes.")
	lsS3Uris        = newS3List(ls.Arg("s3Uris", "list of S3 URIs").Required())
	lsRecurse       = ls.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	lsWithDate      = ls.Flag("with-date", "Include the last modified date.").Short('d').Bool()
	lsDelimiter     = ls.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	lsHumanReadable = ls.Flag("human-readable", "Output human-readable object sizes.").Short('H').Bool()
	lsSearchDepth   = ls.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	lsKeyRegex      = ls.Flag("key-regex", "Regex filter for keys.").Default("").String()

	stream               = app.Command("stream", "Stream S3 files to stdout")
	streamS3Uris         = newS3List(stream.Arg("s3Uris", "list of S3 URIs").Required())
	streamKeyRegex       = stream.Flag("key-regex", "Regex filter for keys").Default("").String()
	streamDelimiter      = stream.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	streamIncludeKeyName = stream.Flag("include-key-name", "Regex filter for keys.").Bool()
	streamSearchDepth    = stream.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	streamOrdered        = stream.Flag("ordered", "Reads the keys in-order, not mixing output from different keys. Note: this will reduce the parallelism to 1").Bool()
	streamRaw            = stream.Flag("raw", "Raw file output, output will not be line delimited or uncompressed").Bool()

	get             = app.Command("get", "Fetch files from S3")
	getS3Uris       = newS3List(get.Arg("s3Uris", "list of S3 URIs").Required())
	getRecurse      = get.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	getDelimiter    = get.Flag("delimiter", "Delimiter to use while listing.").Default("/").String()
	getSearchDepth  = get.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	getKeyRegex     = get.Flag("key-regex", "Regex filter for keys.").Default("").String()
	getSkipExisting = get.Flag("skip-existing", "Skips downloading keys which already exist on the local file system").Bool()

	cp            = app.Command("cp", "Copy files within S3")
	cpS3Uris      = newS3List(cp.Arg("s3Uris", "list of S3 URIs").Required())
	cpRecurse     = cp.Flag("recursive", "Copy all keys for this prefix.").Short('r').Bool()
	cpFlat        = cp.Flag("flat", "Copy all source files into a flat destination folder (vs. corresponding subfolders)").Short('f').Bool()
	cpDelimiter   = cp.Flag("delimiter", "Delimiter to use while copying.").Default("/").String()
	cpSearchDepth = cp.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	cpKeyRegex    = cp.Flag("key-regex", "Regex filter for keys.").Default("").String()

	rm            = app.Command("rm", "Delete files within S3.")
	rmS3Uris      = newS3List(rm.Arg("s3Uris", "list of S3 URIs").Required())
	rmRecurse     = rm.Flag("recursive", "Delete all keys for this prefix.").Short('r').Bool()
	rmDelimiter   = rm.Flag("delimiter", "Delimiter to use while deleting.").Default("/").String()
	rmSearchDepth = rm.Flag("search-depth", "Dictates how many prefix groups to walk down.").Default("0").Int()
	rmKeyRegex    = rm.Flag("key-regex", "Regex filter for keys.").Default("").String()

	version = "master"
)

// Ls lists S3 keys and prefixes using svc, s3Uris specifies which S3 prefixes/keys to list, recursive tells whether or not to list everything
// under s3Uris, delimiter tells which character to use as the delimiter for listing prefixes, searchDepth determines how many prefixes to list
// before parallelizing list calls, keyRegex is a regex filter on Keys
func Ls(svc *s3.S3, s3Uris []string, recursive bool, delimiter string, searchDepth int, keyRegex *string) (chan *s3wrapper.ListOutput, error) {
	wrap := s3wrapper.New(svc, *maxParallel)
	outChan := make(chan *s3wrapper.ListOutput, 10000)

	slashRegex := regexp.MustCompile("/")
	bucketExpandedS3Uris := make([]string, 0, 1000)

	// transforms uris with partial or no bucket (e.g. s3://)
	// into a listable uri
	for _, uri := range s3Uris {
		// filters uris without bucket or partial bucket specified
		// s3 key/prefix queries will always have 3 slashes, where-as
		// bucket queries will always have 2 (e.g. s3://<bucket>/<prefix-or-key> vs s3://<bucket-prefix>)
		if len(slashRegex.FindAllString(uri, -1)) == 2 {
			buckets, err := wrap.ListBuckets(uri)
			if err != nil {
				return nil, err
			}
			for _, bucket := range buckets {
				// add the bucket back to the list of s3 uris in cases where
				// we are searching beyond the bucket
				if recursive || searchDepth > 0 {
					resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
					if err != nil {
						return nil, err
					}
					// if the region is location constrained and not in the region we specified in our config
					// then don't list it, otherwise we will get an error from the AWS API
					if resp.LocationConstraint == nil || *resp.LocationConstraint == *svc.Client.Config.Region {
						bucketExpandedS3Uris = append(bucketExpandedS3Uris, s3wrapper.FormatS3Uri(bucket, ""))
					}
				} else {
					key := ""
					fullKey := s3wrapper.FormatS3Uri(bucket, "")
					outChan <- &s3wrapper.ListOutput{
						IsPrefix:     true,
						Key:          key,
						FullKey:      fullKey,
						LastModified: time.Time{},
						Size:         0,
						Bucket:       bucket,
					}
				}
			}
		} else {
			bucketExpandedS3Uris = append(bucketExpandedS3Uris, uri)
		}
	}
	s3Uris = bucketExpandedS3Uris

	go func() {
		defer close(outChan)

		for i := 0; i < searchDepth; i++ {
			newS3Uris := make([]string, 0)
			for itm := range wrap.ListAll(s3Uris, false, delimiter, keyRegex) {
				if itm.IsPrefix {
					newS3Uris = append(newS3Uris, strings.TrimRight(itm.FullKey, delimiter)+delimiter)
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

func printLs(listChan chan *s3wrapper.ListOutput, humanReadable bool, includeDates bool) {
	for listOutput := range listChan {
		if listOutput.IsPrefix {
			fmt.Printf("%10s %s\n", "DIR", listOutput.FullKey)
		} else {
			var size string
			if humanReadable {
				size = fmt.Sprintf("%10s", humanize.Bytes(uint64(listOutput.Size)))
			} else {
				size = fmt.Sprintf("%10d", listOutput.Size)
			}
			date := ""
			if includeDates {
				date = " " + (listOutput.LastModified).Format("2006-01-02T15:04:05")
			}
			fmt.Printf("%s%s %s\n", size, date, listOutput.FullKey)
		}
	}
}

// Stream streams S3 Key content to stdout using the svc, s3Uris specifies the S3 Prefixes/Keys to stream, delimiter tells the delimiter to use
// when listing, searchDepth determines how many prefixes to list before parallelizing list calls, includeKeyName will prefix each line with the
// key in which the line came from, keyRegex is a regex filter on Keys, ordered determines whether the lines can be inter-mingled with lines from
// other files or must be in order (helpful for parsing binary files), raw is a boolean for determining whether to output the raw data of each
// file instead of lines
func Stream(svc *s3.S3, s3Uris []string, delimiter string, searchDepth int, includeKeyName bool, keyRegex *string, ordered bool, raw bool) error {
	listCh, err := Ls(svc, s3Uris, true, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}
	wrap := s3wrapper.New(svc, *maxParallel)
	if ordered {
		wrap.WithMaxConcurrency(1)
	}

	lines := wrap.Stream(listCh, includeKeyName, raw)
	for line := range lines {
		fmt.Print(line)
	}

	return nil
}

// Get downloads a file to the local filesystem using svc, s3Uris specifies the S3 Prefixes/Keys to download, recurse tells whether or not
// to download everything under s3Uris, delimiter tells the delimiter to use when listing,
// searchDepth determines how many prefixes to list before parallelizing list calls, keyRegex is a
// regex filter on Keys, skipExisting skips files which already exist on the filesystem.
func Get(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex *string, skipExisting bool) error {
	listCh, err := Ls(svc, s3Uris, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, *maxParallel)

	downloadedFiles := wrap.GetAll(listCh, skipExisting)
	for file := range downloadedFiles {
		fmt.Printf("Downloaded %s -> %s\n", file.FullKey, file.Key)
	}

	return nil
}

// Cp copies files from one s3 location to another using svc, s3Uris is a list of source and dest s3 URIs, recurse tells
// whether to list all keys under the source prefix,  delimiter tells the delimiter to use when listing, searchDepth determines
// the number of prefixes to list before parallelizing list calls, keyRegex is a regex filter on keys, when flat is
// true it only takes the last part of the prefix as the filename.
func Cp(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex *string, flat bool) error {
	if len(s3Uris) != 2 {
		fmt.Println("fasts3: error: must include one source and one destination URI")
		os.Exit(1)
	}

	listCh, err := Ls(svc, []string{s3Uris[0]}, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, *maxParallel)

	copiedFiles := wrap.CopyAll(listCh, s3Uris[0], s3Uris[1], delimiter, recurse, flat)
	for file := range copiedFiles {
		fmt.Printf("Copied %s -> %s%s%s\n", file.FullKey, strings.TrimRight(s3Uris[1], delimiter), delimiter, file.Key)
	}

	return nil
}

// Rm removes files from S3 using svc, s3Uris is a list of prefixes/keys to delete, recurse tells whether or not to delete
// everything under the prefixes, delimiter tells the delimiter to use when listing, searchDepth determines the number of
// prefixes to list before parallelizing list calls, keyRegex is a regex filter on keys
func Rm(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex *string) error {
	listCh, err := Ls(svc, s3Uris, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, *maxParallel)
	deleted := wrap.DeleteObjects(listCh)
	for key := range deleted {
		fmt.Printf("Deleted %s\n", key.FullKey)
	}
	return nil
}

func main() {
	app.Version(version)
	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
		return
	}

	svc := s3.New(awsSession, aws.NewConfig())

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	// Register user
	case ls.FullCommand():
		listCh, err := Ls(svc, *lsS3Uris, *lsRecurse, *lsDelimiter, *lsSearchDepth, lsKeyRegex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
		printLs(listCh, *lsHumanReadable, *lsWithDate)

	case stream.FullCommand():
		err := Stream(svc, *streamS3Uris, *streamDelimiter, *streamSearchDepth, *streamIncludeKeyName, streamKeyRegex, *streamOrdered, *streamRaw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
	case get.FullCommand():
		Get(svc, *getS3Uris, *getRecurse, *getDelimiter, *getSearchDepth, getKeyRegex, *getSkipExisting)
	case cp.FullCommand():
		if err := Cp(svc, *cpS3Uris, *cpRecurse, *cpDelimiter, *cpSearchDepth, cpKeyRegex, *cpFlat); err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
	case rm.FullCommand():
		if err := Rm(svc, *rmS3Uris, *rmRecurse, *rmDelimiter, *rmSearchDepth, rmKeyRegex); err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
	}
}

type s3List []string

// newS3List creates a new s3List kingpin setting
func newS3List(s kingpin.Settings) *s3List {
	target := new(s3List)
	s.SetValue(target)
	return target
}

// Set overrides kingping's Set method to validate value for s3 URIs
func (s *s3List) Set(value string) error {
	hasMatch, err := regexp.MatchString("^s3://", value)
	if err != nil {
		return err
	}
	if !hasMatch {
		return fmt.Errorf("%s not a valid S3 uri, Please enter a valid S3 uri. Ex: s3://mary/had/a/little/lamb", value)
	}
	*s = append(*s, value)
	return nil
}

func (s *s3List) String() string {
	return ""
}

// IsCumulative specifies S3List as a cumulative argument
func (s *s3List) IsCumulative() bool {
	return true
}
