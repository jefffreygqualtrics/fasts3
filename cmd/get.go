package cmd

import (
	"log"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"github.com/tuneinc/fasts3/s3wrapper"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <S3 URIs>",
	Short: "Download files from S3",
	Long:  ``,
	Args:  validateS3URIs(cobra.MinimumNArgs(1)),
	Run: func(cmd *cobra.Command, args []string) {
		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			log.Fatal(err)
		}
		skipExisting, err := cmd.Flags().GetBool("skip-existing")
		if err != nil {
			log.Fatal(err)
		}
		err = Get(s3Client, args, recursive, delimiter, searchDepth, keyRegex, skipExisting)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	getCmd.Flags().BoolP("recursive", "r", false, "Get all keys for this prefix")
	getCmd.Flags().BoolP("skip-existing", "x", false, "Skips downloading keys which already exist on the local file system")
}

// Get downloads a file to the local filesystem using svc, s3Uris specifies the
// S3 Prefixes/Keys to download, recurse tells whether or not to download
// everything under s3Uris, delimiter tells the delimiter to use when listing,
// searchDepth determines how many prefixes to list before parallelizing list
// calls, keyRegex is a regex filter on Keys, skipExisting skips files which
// already exist on the filesystem.
func Get(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex string, skipExisting bool) error {
	listCh, err := Ls(svc, s3Uris, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, maxParallel)

	downloadedFiles := wrap.GetAll(listCh, skipExisting)
	for file := range downloadedFiles {
		log.Printf("Downloaded %s -> %s\n", file.FullKey, file.Key)
	}

	return nil
}
