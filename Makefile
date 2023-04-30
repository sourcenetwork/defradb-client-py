# Generating Python gRPC code from a protobuf definition file
# from the latest commit of DefraDB's develop branch

REPO_NAME = _defradb
REPO = git@github.com:sourcenetwork/defradb.git
PROTO_DIR = net/api/pb
PROTO_FILE = api.proto
PYTHON_OUT = src/defradb/rpc

protobufs: clone
	mkdir -p $(PYTHON_OUT)
	python -m grpc_tools.protoc -I $(REPO_NAME)/$(PROTO_DIR) --python_out=$(PYTHON_OUT) --grpc_python_out=$(PYTHON_OUT) $(PROTO_FILE) 

clone:
	if [ ! -d $(REPO) ]; then \
		git clone $(REPO) $(REPO_NAME) || true; \
	fi
	cd $(REPO_NAME) && git pull origin master && cd -
