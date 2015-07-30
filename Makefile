all: fasts3

fasts3: *.go
	GOPATH=$(shell pwd) go get github.com/AdRoll/goamz/aws
	GOPATH=$(shell pwd) go get github.com/AdRoll/goamz/s3
	GOPATH=$(shell pwd) go get github.com/alecthomas/kingpin
	GOPATH=$(shell pwd) go get github.com/dustin/go-humanize
	GOPATH=$(shell pwd) go get github.com/vaughan0/go-ini
	mkdir -p src/github.com/TuneOSS/fasts3/
	cp -r --parent *.go */*.go src/github.com/TuneOSS/fasts3/
	cd src/github.com/TuneOSS/fasts3/
	GOPATH=$(shell pwd) go build

build:
	GOPATH=$(shell pwd) go build

clean:
	 rm -rf bin pkg src com
	 rm -f fasts3

install:
	 cp -f fasts3 /usr/local/bin/fasts3
