current_dir = $(shell pwd)
all:
	#go build -o nwfs_client client/*.go
	#go build -o nwfs_back   backend/*.go
	go env -w GOPATH=$(current_dir)
	go env -w GOBIN=$(current_dir)/bin
	go install ./...

clean:
	rm -r bin/* pkg/*

