package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"github.com/tuneinc/fasts3/s3wrapper"
)

// streamCmd represents the stream command
var streamCmd = &cobra.Command{
	Use:   "stream <S3 URIs>",
	Short: "Stream the S3 objects contents to STDOUT",
	Args:  validateS3URIs(cobra.MinimumNArgs(1)),
	Run: func(cmd *cobra.Command, args []string) {
		includeKeyName, err := cmd.Flags().GetBool("include-key-name")
		if err != nil {
			log.Fatal(err)
		}
		ordered, err := cmd.Flags().GetBool("ordered")
		if err != nil {
			log.Fatal(err)
		}
		raw, err := cmd.Flags().GetBool("raw")
		if err != nil {
			log.Fatal(err)
		}

		err = Stream(
			s3Client,
			args,
			delimiter,
			searchDepth,
			includeKeyName,
			keyRegex,
			ordered,
			raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
	},
}

// Stream streams S3 Key content to stdout using the svc, s3Uris specifies the
// S3 Prefixes/Keys to stream, delimiter tells the delimiter to use when listing,
// searchDepth determines how many prefixes to list before parallelizing list
// calls, includeKeyName will prefix each line with the key in which the line
// came from, keyRegex is a regex filter on Keys, ordered determines whether the
// lines can be inter-mingled with lines from other files or must be in order
// (helpful for parsing binary files), raw is a boolean for determining whether
// to output the raw data of each file instead of lines
func Stream(
	svc *s3.S3,
	s3Uris []string,
	delimiter string,
	searchDepth int,
	includeKeyName bool,
	keyRegex string,
	ordered bool,
	raw bool,
) error {
	listCh, err := Ls(svc, s3Uris, true, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}
	wrap := s3wrapper.New(svc, maxParallel)
	if ordered {
		wrap.WithMaxConcurrency(1)
	}

	lines := wrap.Stream(listCh, includeKeyName, raw)
	for line := range lines {
		fmt.Print(line)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(streamCmd)

	streamCmd.Flags().BoolP("include-key-name", "i", false, "Include the key name in streamed output")
	streamCmd.Flags().BoolP("ordered", "o", false, "Read the keys in-order, not mixing output from different keys (this will reduce the parallelism to 1)")
	streamCmd.Flags().BoolP("raw", "r", false, "Raw object stream (do not uncompress or delimit stream)")
}
