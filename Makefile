all:
	go build -o nwfs_client client/*.go
	go build -o nwfs_back   backend/*.go

clean:
	rm  nwfs_client nwfs_back

