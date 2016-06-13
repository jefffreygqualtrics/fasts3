SHELL := /bin/bash

all: build

build:
	$(GOROOT)/bin/go build

clean:
	 rm -rf bin pkg src com
	 rm -f fasts3

install:
	 cp -f fasts3 /usr/local/bin/fasts3
