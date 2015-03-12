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
usage: fasts3 [<flags>] <command> [<flags>] [<args> ...]

Multi-threaded s3 utility

Flags:
  --help  Show help.

Commands:
  help [<flags>] <command>
    Show help for a command.

  ls [<flags>] <s3uri>
    List s3 prefixes.

  del [<flags>] [<prefixes>]
    Delete s3 keys

  init [<flags>]
    Initialize .fs3cfg file in home directory

```

#####ls
```
fasts3 [<flags>] ls [<flags>] <s3uri>

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
  [<prefixes>]  partial s3 uri to del all keys under it
```
