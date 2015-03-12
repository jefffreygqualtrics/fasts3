package awswrapper

import (
	"os"
	"os/user"
	"path"

	"github.com/AdRoll/goamz/aws"
	"github.com/vaughan0/go-ini"
)

func GetAwsAuth() (error, aws.Auth) {
	env_access_key_id := os.Getenv("AWS_ACCESS_KEY_ID")
	env_secret_key_id := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if env_access_key_id != "" && env_secret_key_id != "" {
		return nil, aws.Auth{AccessKey: env_access_key_id, SecretKey: env_secret_key_id}
	}

	usr, err := user.Current()
	if err != nil {
		return err, aws.Auth{}
	}

	file, err := ini.LoadFile(path.Join(usr.HomeDir, ".fs3cfg"))
	if err != nil {
		return err, aws.Auth{}
	} else {
		return nil, aws.Auth{AccessKey: file["default"]["access_key"], SecretKey: file["default"]["secret_key"]}
	}
}
