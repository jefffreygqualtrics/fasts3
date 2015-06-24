package awswrapper

import (
	"os"
	"os/user"
	"path"

	"github.com/AdRoll/goamz/aws"
	"github.com/vaughan0/go-ini"
)

func GetAwsAuth() (aws.Auth, error) {
	env_access_key_id := os.Getenv("AWS_ACCESS_KEY_ID")
	env_secret_key_id := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if env_access_key_id != "" && env_secret_key_id != "" {
		return aws.Auth{AccessKey: env_access_key_id, SecretKey: env_secret_key_id}, nil
	}

	usr, err := user.Current()
	if err != nil {
		return aws.Auth{}, err
	}

	file, err := ini.LoadFile(path.Join(usr.HomeDir, ".fs3cfg"))
	if err != nil {
		// try looking for .s3cfg (common for s3cmd users)
		file, err = ini.LoadFile(path.Join(usr.HomeDir, ".s3cfg"))
		if err != nil {
			return aws.Auth{}, err
		}
	}
	return aws.Auth{AccessKey: file["default"]["access_key"], SecretKey: file["default"]["secret_key"]}, nil
}
