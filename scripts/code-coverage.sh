#!/bin/bash
list=$(go list ./...)
i=1
for pkg in $list
do
    go test -coverprofile=pkg-$i.cover.out -coverpkg=./... -covermode=atomic $pkg
    i=$((i+1))
done
mkdir -p tmp
echo "mode: set" > tmp/coverage.out && cat *.cover.out | grep -v mode: | sort -r | \
awk '{if($1 != last) {print $0;last=$1}}' >> tmp/coverage.out
