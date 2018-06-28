package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"github.com/tuneinc/fasts3/s3wrapper"
)

// rmCmd represents the rm command
var rmCmd = &cobra.Command{
	Use:   "rm <S3 URIs>",
	Short: "Delete files within S3",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("rm called")
		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			log.Fatal(err)
		}
		if err := Rm(s3Client, args, recursive, delimiter, searchDepth, keyRegex); err != nil {
			fmt.Fprintf(os.Stderr, "Encountered an error: %s\n", err)
			return
		}
	},
}

// Rm removes files from S3 using svc, s3Uris is a list of prefixes/keys to delete, recurse tells whether or not to delete
// everything under the prefixes, delimiter tells the delimiter to use when listing, searchDepth determines the number of
// prefixes to list before parallelizing list calls, keyRegex is a regex filter on keys
func Rm(svc *s3.S3, s3Uris []string, recurse bool, delimiter string, searchDepth int, keyRegex string) error {
	listCh, err := Ls(svc, s3Uris, recurse, delimiter, searchDepth, keyRegex)
	if err != nil {
		return err
	}

	wrap := s3wrapper.New(svc, maxParallel)
	deleted := wrap.DeleteObjects(listCh)
	for key := range deleted {
		fmt.Printf("Deleted %s\n", key.FullKey)
	}
	return nil
}

func init() {
	rootCmd.AddCommand(rmCmd)

	rmCmd.Flags().BoolP("recursive", "r", false, "Get all keys for this prefix")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rmCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// rmCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
