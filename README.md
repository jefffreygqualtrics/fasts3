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
```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>
```

#Usage

```
usage: fasts3 <command> [<flags>] [<args> ...]

Multi-threaded s3 utility

Flags:
  --help  Show help.

Commands:
  help [<command>]
    Show help for a command.

  ls [<flags>] <s3uri>
    List s3 prefixes.

  del [<flags>] [<prefixes>]
    Delete s3 keys

  get [<flags>] [<prefixes>]
    Fetch files from s3

  stream [<flags>] [<prefixes>]
    Stream s3 files to stdout

  init
    Initialize .fs3cfg file in home directory

```

#####ls
```
usage: fasts3 [<flags>] ls [<flags>] <s3uri>

List s3 prefixes.

Flags:
  --help            Show help.
  -r, --recursive   Get all keys for this prefix.
  --search-depth=0  search depth to search for work.
  -H, --human-readable  human readable key size.
  -d, --with-date   include the last modified date.

Args:
  <s3uri>  paritial s3 uri to list, ex: s3://mary/had/a/little/lamb/

```

#####del
```
usage: fasts3 [<flags>] del [<flags>] [<prefixes>]

Delete s3 keys

Flags:
  --help            Show help.
  -r, --recursive   Delete all keys with prefix
  --search-depth=0  search depth to search for work.

Args:
  [<prefixes>]  1 or more partial s3 uris to delete delimited by space

```

#####get
```
usage: fasts3 get [<flags>] [<prefixes>]

Fetch files from s3

Flags:
  --search-depth=0  search depth to search for work.

Args:
  [<prefixes>]  list of prefixes or s3Uris to retrieve
```

#####stream
```
usage: fasts3 stream [<flags>] [<prefixes>]

Stream s3 files to stdout

Flags:
  --search-depth=0  search depth to search for work.
  --key-regex=KEY-REGEX
                    regex filter for keys

Args:
  [<prefixes>]  list of prefixes or s3Uris to retrieve
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

# del
fasts3 del -r s3://mybuck/logs/ # deletes all keys in the prefix
fasts3 del s3://mybuck/logs/2015/01/12/api.log.201501122359.gz # deletes single key
fasts3 del $(fasts3 ls s3://mybuck/logs/2015/01/12 | awk -F " " '/api.log/{print $2}') # delete all keys that have "api.log" in them

#get
fasts3 get s3://mybuck/logs/ # fetches all logs in the prefix

# stream
fasts3 stream s3://mybuck/logs/ # streams all logs under prefix to stdout
fasts3 stream --key-filter ".*2015-01-01" s3://mybuck/logs/ # streams all logs with 2015-01-01 in the key name stdout
```
