package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"github.com/tuneinc/fasts3/s3wrapper"
)

// cpCmd represents the cp command
var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: "Copy files within S3",
	Long:  ``,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			log.Fatal(err)
		}
		flat, err := cmd.Flags().GetBool("flat")
		if err != nil {
			log.Fatal(err)
		}
		err = Cp(s3Client, args, recursive, delimiter, searchDepth, keyRegex, flat)
		if err != nil {
			log.Fatal(err)
		}
	},
}

// Cp copies files from one s3 location to another using svc, s3Uris is a list of source and dest s3 URIs, recurse tells
// whether to list all keys under the source prefix,  delimiter tells the delimiter to use when listing, searchDepth determines
// the number of prefixes to list before parallelizing list calls, keyRegex is a regex filter on keys, when flat is
// true it only takes the last part of the prefix as the filename.
func Cp(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex string, flat bool) error {
	if len(s3Uris) != 2 {
		fmt.Println("fasts3: error: must include one source and one destination URI")
		os.Exit(1)
	}

	listCh, err := Ls(svc, []string{s3Uris[0]}, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, maxParallel)

	copiedFiles := wrap.CopyAll(listCh, s3Uris[0], s3Uris[1], delimiter, recurse, flat)
	for file := range copiedFiles {
		fmt.Printf("Copied %s -> %s%s%s\n", file.FullKey, strings.TrimRight(s3Uris[1], delimiter), delimiter, file.Key)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(cpCmd)

	cpCmd.Flags().BoolP("recursive", "r", false, "Copy all keys for this prefix.")
	cpCmd.Flags().BoolP("flat", "f", false, "Copy all source files into a flat destination folder (vs. corresponding subfolders)")
}
