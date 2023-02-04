all: goproto

goproto:
	protoc --proto_path=proto --go_out=proto --go_opt=paths=source_relative proto\shmem.proto