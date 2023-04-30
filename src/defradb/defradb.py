import json
import logging
from dataclasses import dataclass

import base58
import grpc
import multiaddr
import multiaddr.protocols
import requests
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from graphql import DocumentNode

from .rpc import api_pb2, api_pb2_grpc

ROUTE_GRAPHQL = "graphql"
ROUTE_SCHEMA_LOAD = "schema/load"
ROUTE_PEERID = "peerid"


@dataclass
class DefraConfig:
    """
    Configuration for DefraDB client.
    """

    api_url: str = "localhost:9181/api/v0/"
    tcp_multiaddr: str = "localhost:9161"
    scheme = "http://"


class DefraClient:
    """
    Client for DefraDB, providing methods for interacting with the DefraDB node.
    Interactions with DefraDB via the graphql endpoint use the Defra query language, by
    passing a valid gql query object.
    Interactions with DefraDB via the rpc endpoint use the Defra protobuf API.
    This is the synchronous client.
    """

    def __init__(self, cfg):
        self.cfg: DefraConfig = cfg
        url = f"{self.cfg.scheme}{self.cfg.api_url}{ROUTE_GRAPHQL}"
        self.gql_sync_transport = RequestsHTTPTransport(url=url)

    def request(self, request: DocumentNode):
        """
        Execute a graphql request against the DefraDB node.
        """
        response = None
        client = Client(
            transport=self.gql_sync_transport, fetch_schema_from_transport=False
        )
        response = client.execute(request)
        return response

    def load_schema(self, schema: str):
        """
        Load a schema into the DefraDB node.
        """
        url = f"{self.cfg.scheme}{self.cfg.api_url}{ROUTE_SCHEMA_LOAD}"
        response = requests.post(url, data=schema)
        response_json = response.json()
        if "errors" in response_json:
            for error in response_json["errors"]:
                schema_already_exists = "schema type already exists" in error["message"]
                if schema_already_exists:
                    logging.info("Schema already exists")
                else:
                    raise Exception("Failed to load schema", error)
        return response_json

    def create_doc(self, typename: str, data: dict):
        """
        Create a document in the DefraDB node.
        """
        data_string = json.dumps(data).replace('"', '\\"')
        request = f"""mutation {{
            create_{typename} (data: "{data_string}") {{
                _key
            }}
        }}"""
        response = self.request(gql(request))
        return response

    def set_replicator(self, collections: list[str], maddr: str) -> str:
        """
        Set a replicator in the DefraDB node.
        """
        client = self._get_rpc_client(self.cfg.tcp_multiaddr)
        maddr = multiaddr.Multiaddr(maddr)
        request = api_pb2.SetReplicatorRequest(  # type: ignore
            collections=collections, addr=maddr.to_bytes()
        )
        response = client.SetReplicator(request)
        peerIDstr = str(base58.b58encode(response.peerID))
        return peerIDstr

    def delete_replicator(self, peerID: str) -> str:
        """
        Delete a replicator in the DefraDB node.
        """
        client = self._get_rpc_client(self.cfg.tcp_multiaddr)
        peerIDbytes = base58.b58decode(peerID)
        request = api_pb2.DeleteReplicatorRequest(peerID=peerIDbytes)  # type: ignore
        response = client.DeleteReplicator(request)
        response_peerIDstr = str(base58.b58encode(response.peerID))
        return response_peerIDstr

    def get_all_replicators(self) -> list:
        """
        Get all replicators in the DefraDB node.
        """
        client = self._get_rpc_client(self.cfg.tcp_multiaddr)
        request = api_pb2.GetAllReplicatorRequest()  # type: ignore
        response = client.GetAllReplicators(request)
        replicators = []
        for r in response.replicators:
            replicator_id = base58.b58encode(r.info.id).decode()
            addrs = multiaddr.Multiaddr(r.info.addrs)
            replicators.append(
                {"id": replicator_id, "addrs": addrs, "schemas": [s for s in r.schemas]}
            )
        return replicators

    def _get_rpc_client(self, addr: str) -> api_pb2_grpc.ServiceStub:
        addr = _multiaddr_to_porthost(addr)
        channel = grpc.insecure_channel(addr)
        clientrpc = api_pb2_grpc.ServiceStub(channel)
        return clientrpc

    def _get_peerid(self) -> str:
        url = f"{self.cfg.scheme}{self.cfg.api_url}{ROUTE_PEERID}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to get peerid", response.text)
        peerID = response.json()["data"]["peerID"]
        return peerID


def dict_to_create_query(schema_type: str, data: dict) -> DocumentNode:
    """
    Create a mutation to create a new document of a specific type.
    """
    data_json = json.dumps(data, ensure_ascii=False).replace('"', '\\"')
    request_string = f"""
        mutation {{
            create_{schema_type}(data: "{data_json}") {{
                _key
            }}
        }}
    """
    return gql(request_string)


def dict_to_update_query(schema_type: str, data: dict) -> DocumentNode:
    """
    Create a mutation to create a new document of a specific type.
    """
    data_json = json.dumps(data, ensure_ascii=False).replace('"', '\\"')
    request_string = f"""
        mutation {{
            update_{schema_type}(data: "{data_json}") {{
                _key
            }}
        }}
    """
    return gql(request_string)


def _multiaddr_to_porthost(maddr: str) -> str:
    m = multiaddr.Multiaddr(maddr)
    ip = m.value_for_protocol(multiaddr.protocols.P_IP4)
    port = m.value_for_protocol(multiaddr.protocols.P_TCP)
    return f"{ip}:{port}"
