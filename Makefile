PROTOC ?= protoc
OUT_DIR ?= .

PROTO_FILES = \
	pkg/proto/ws_frames.proto \
	pkg/proto/user.proto \
	pkg/proto/chat.proto \
	pkg/proto/media.proto \
	pkg/proto/call.proto \
	pkg/proto/presence.proto

.PHONY: proto
proto:
	$(PROTOC) $(PROTO_FILES) \
		--go_out=$(OUT_DIR) --go_opt=module=wire-server \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=module=wire-server