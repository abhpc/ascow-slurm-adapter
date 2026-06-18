ARCH ?= amd64

protos:
	buf generate --template buf.gen.yaml https://github.com/abhpc/ascow-scheduler-adapter-interface.git#subdir=protos

run: 
	go run *.go 

build:
	go get scow-slurm-adapter
	CGO_BUILD=0 GOARCH=${ARCH} go build -o scow-slurm-adapter-${ARCH}

test:
	go test
