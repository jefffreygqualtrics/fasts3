![FastS3](http://i.imgur.com/A42azaA.png)
---

Fast s3 utility is a faster version of s3cmd's ls and get functions ideal for listing and deleting buckets containing millions of keys.

#Installation

```bash
go get github.com/TuneOSS/fasts3
cd $GOPATH/src/github.com/TuneOSS/fasts3
make
sudo make install
```

#Configuration

use `aws configure` command from the aws cli tool (https://aws.amazon.com/cli/) which will create the necessary config files in ~/.aws/credentials

alternatively you can set these environment variables which will take precedence over the credentials file:
```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>
```

#Usage
use:
```
fasts3 --help
fasts3 <cmd> --help
```
####Using search depth to *go* faster
Many times you know the structure of your s3 bucket, this can be used to optimize listings. Say you have a structure like so:
```bash
fasts3 ls s3://mybuck/logs/

DIR s3://mybuck/logs/2010/
DIR s3://mybuck/logs/2012/
DIR s3://mybuck/logs/2013/
DIR s3://mybuck/logs/2014/
DIR s3://mybuck/logs/2015/
```

doing a `fasts3 ls -r s3://mybuck/logs/` will read all keys under `logs` sequentially. We can make this faster by adding a `--search-depth 1` flag to the command which gives each of the underlying directories it's own thread increasing throughput.

####Examples
```bash
# ls
fasts3 ls s3://mybucket/ # lists top level directories and keys
fasts3 ls -r s3://mybucket/ # lists all keys in the bucket
fasts3 ls -r --search-depth 1 s3://mybucket/ # lists all keys in the bucket using the directories 1 level down to thread
fasts3 ls -r s3://mybucket/ | awk '{s += $1}END{print s}' # sum sizes of all objects in the bucket

# get
fasts3 get s3://mybuck/logs/ # fetches all logs in the prefix

# stream
fasts3 stream s3://mybuck/logs/ # streams all logs under prefix to stdout
fasts3 stream --key-regex ".*2015-01-01" s3://mybuck/logs/ # streams all logs with 2015-01-01 in the key name stdout
```

###Completion
Bash and ZSH completion are available, to install:

for bash:
```
source completion.sh
```

for zsh:
```
source completion.zsh
```
