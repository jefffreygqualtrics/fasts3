package cmd

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cobra",
	Short: "A faster S3 utility",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if showVersion, err := cmd.Flags().GetBool("version"); err == nil && showVersion {
			versionCmd.Run(cmd, args)
			return
		}
	},
}

var (
	s3Client *s3.S3

	keyRegex    string
	delimiter   string
	searchDepth int
	maxParallel int
)

func init() {
	rootCmd.PersistentFlags().Bool("version", false, "Show the version")
	rootCmd.PersistentFlags().StringVar(&keyRegex, "key-regex", "", "Regex filter for keys")
	rootCmd.PersistentFlags().StringVar(&delimiter, "delimiter", "/", "Delimiter to use while listing")
	rootCmd.PersistentFlags().IntVar(&searchDepth, "search-depth", 0, "Dictates how many prefix groups to walk down")
	rootCmd.PersistentFlags().IntVarP(&maxParallel, "max-parallel", "p", 10, "Maximum number of calls to make to S3 simultaneously")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatal(err)
	}
	s3Client = s3.New(awsSession, aws.NewConfig())

	cmd, err := rootCmd.ExecuteC()
	if err != nil {
		log.Fatal(err)
	}

	// execute the help if called with no child command
	if cmd == rootCmd {
		rootCmd.Help()
	}
}
