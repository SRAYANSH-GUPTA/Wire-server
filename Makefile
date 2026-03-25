PROTOC ?= protoc
OUT_DIR ?= pkg/proto

PROTO_FILES = \
	pkg/proto/ws_frames.proto \
	pkg/proto/user.proto \
	pkg/proto/chat.proto \
	pkg/proto/media.proto \
	pkg/proto/call.proto \
	pkg/proto/presence.proto

GO_OUT = --go_out=$(OUT_DIR) --go_opt=paths=source_relative
GRPC_GO_OUT = --go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative

.PHONY: proto
proto:
	$(PROTOC) $(PROTO_FILES) $(GO_OUT) $(GRPC_GO_OUT)
