#FastS3 utility

Fast s3 utility is a faster version of s3cmd's ls and del functions ideal for listing and deleting buckets containing millions of keys.

#Installation

```bash
go get github.com/TuneOSS/fasts3
cd $GOPATH/src/github.com/TuneOSS/fasts3
go build
```

#Configuration

use `fasts3 init` command  which will create a template file in ~/.fs3cfg

```ini
[default]
access_key=<access_key>
secret_key=<secret_key>
```

fill in the template file with your s3 credentials

alternatively you can set these environment variables:
*AWS_ACCESS_KEY_ID*
*AWS_SECRET_ACCESS_KEY*

