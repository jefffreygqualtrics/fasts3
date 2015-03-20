package awswrapper

import (
	"os"
	"os/user"
	"path"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/vaughan0/go-ini"
)

func GetAwsAuth() (error, aws.CredentialsProvider) {
	env_access_key_id := os.Getenv("AWS_ACCESS_KEY_ID")
	env_secret_key_id := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if env_access_key_id != "" && env_secret_key_id != "" {
		return nil, aws.Creds(env_access_key_id, env_secret_key_id, "")
	}

	usr, err := user.Current()
	if err != nil {
		return err, nil
	}

	file, err := ini.LoadFile(path.Join(usr.HomeDir, ".fs3cfg"))
	if err != nil {
		return err, nil
	} else {
		return nil, aws.Creds(file["default"]["access_key"], file["default"]["secret_key"], "")
	}
}
