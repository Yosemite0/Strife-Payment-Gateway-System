# Imports
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import protofiles.service_pb2 as service_pb2
import protofiles.service_pb2_grpc as service_pb2_grpc

import grpc
import consul
import json
import os
from concurrent import futures
import base64
from typing import Dict, Tuple
import threading
import jwt
import time
import logging
from typing import Tuple, Optional
import signal

# Constants
CONSUL_CLIENT = None
ADDRESS = None
CERT  = None
ENCODED_CERT = None
KEY = None

NAME = "gateway"

ALGORITHM = "HS256"

JWT_SECRET_KEY="This is a very very secret key. don't use Anywhere"
TOKEN_EXPIRY = 3600

TIMEOUT2PC = None

## Data structures
bank_lock = threading.Lock()
user_lock = threading.Lock()
token_lock = threading.Lock()

banks = dict() # bank_id -> (address, certificate)
users = dict() # (user_id, bank) -> (password, lock)
tokens = dict() # (token -> (user_id, bank)) -> token

transaction_lock = threading.Lock()
transaction_ids = set() # Stores transaction ids to prevent replay attacks


class LoggingInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        self.logger = logging.getLogger("gateway_interceptor")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler("gateway_interceptor.log")
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(fh)

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        if handler.unary_unary:
            def logging_unary_unary(request, context):
                method = handler_call_details.method
                print(f"Method called: {method}")
                self.logger.info(f"Request for {method}: {request}")
                response = handler.unary_unary(request, context)
                self.logger.info(f"Response for {method}: {response}")
                return response
            return grpc.unary_unary_rpc_method_handler(
                logging_unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )
        return handler

class JwtInterceptor(grpc.ServerInterceptor):    
    def intercept_service(self, continuation, handler_call_details):
        # Skip token validation for public methods
        if (handler_call_details.method.endswith("Login") or
                handler_call_details.method.endswith("RegisterBank") or
                handler_call_details.method.endswith("Logout")):
            return continuation(handler_call_details)
        
        # Extract metadata as a dict
        metadata = dict(handler_call_details.invocation_metadata)
        token = metadata.get("token")
        if not token:
            def aborter(_request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing authorization token")
            return grpc.unary_unary_rpc_method_handler(aborter)
        
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        except jwt.ExpiredSignatureError:
            def aborter(_request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token expired")
            return grpc.unary_unary_rpc_method_handler(aborter)
        except Exception as e:
            def aborter(_request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, f"Invalid token: {str(e)}")
            return grpc.unary_unary_rpc_method_handler(aborter)
        
        # tokens are valid for 1 hour
        token_issue_time = payload.get("time")
        if time.time() - token_issue_time > TOKEN_EXPIRY:
            def aborter(_request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token expired based on timestamp. Relogin neccessary")
            return grpc.unary_unary_rpc_method_handler(aborter)
        
        # User authorization: ensure the token exists in our token registry
        with token_lock:
            if token not in tokens:
                def aborter(_request, context):
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "Unrecognized or unauthorized token")
                return grpc.unary_unary_rpc_method_handler(aborter)
        
        # All checks passed, forward the call
        return continuation(handler_call_details)


# NEW: Global transaction history at gateway level (if needed)
gateway_tx_history = []  # list of dict(transaction_id, from_user, from_bank, to_user, to_bank, amount)
gateway_tx_lock = threading.Lock()

# Servicer
class GatewayServicer(service_pb2_grpc.GatewayServicer):

    def RegisterBank(self, request, context):
        print(f"Registering bank {request.bank_name}")
        with bank_lock:
            banks[request.bank_name] = (request.bank_address, base64.b64decode(request.bank_cert))
        response = service_pb2.Ack()
        response.success = True
        return response
    
    def Login(self, request, context):
        print("Login request received")
        bank_name = request.bank_name
        user_id = request.username
        password = request.password
        response = service_pb2.Token()

        # Check bank
        channel, ack, err = create_channel(bank_name)

        if not ack:
            response.success = False
            response.token = ""
            response.error = err
            return response

        # Check user
        token = jwt.encode({"user_id": user_id, "bank": bank_name, "time": time.time()}, JWT_SECRET_KEY, algorithm=ALGORITHM)

        if (user_id, bank_name) in users:
            response.success = False
            response.token = ""
            response.error = "User already logged in"
            return response

        # Authenticate user
        stub = service_pb2_grpc.BankStub(channel)
        auth_response = stub.Authenticate(request) 
        if not auth_response.success:
            response.success = False
            response.token = ""
            response.error = auth_response.error
            return response
        
        # Generate token
        with token_lock:
            tokens[token] = (user_id, bank_name)
        with user_lock:
            users[(user_id, bank_name)] = (password, threading.Lock())
        
        response.success = True
        response.token = token
        response.error = ""
        return response

    def Logout(self, request, context):
        print("Logout request received")
        token = request.token
        data = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        user_id = data["user_id"]
        bank_name = data["bank"]
        response = service_pb2.Ack()
        with token_lock:
            if token not in tokens:
                response.success = False
                response.error = "Invalid token"
                return response
            del tokens[token]
        with user_lock:
            if (user_id, bank_name) not in users:
                response.success = False
                response.error = "User not found"
                return response
            del users[(user_id, bank_name)]

        response.success = True
        return response
    
    def ViewBalance(self, request, context):
        token = request.token
        # data = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        (username, bank_name) = tokens[token]
        password, lock = users[(username, bank_name)]

        # Check bank
        channel, success, error = create_channel(bank_name)
        
        if not success:
            response = service_pb2.Balance()
            response.ack.success = False
            response.ack.error = error
            return response
        
        stub = service_pb2_grpc.BankStub(channel)
        auth_request = service_pb2.Auth(username=username, password=password, bank_name=bank_name)
        response = stub.ViewBalance(auth_request)
        return response

    def TransferMoney(self, request, context):
        # Phase 0: Extract token info for source user
        token = request.token
        if token not in tokens:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "Invalid token"
            return ack
        txn_id = request.transaction_id
        with transaction_lock:
            if txn_id in transaction_ids:
                ack = service_pb2.Ack()
                ack.success = False
                ack.error = "Duplicate transaction ID"
                return ack
            else:
                transaction_ids.add(txn_id)
        (from_user, from_bank) = tokens[token]
        amount = request.amount
        # Retrieve password for source user from local registry
        with user_lock:
            if (from_user, from_bank) not in users:
                ack = service_pb2.Ack()
                ack.success = False
                ack.error = "User not logged in"
                return ack
            source_password = users[(from_user, from_bank)][0]

        if request.amount <= 0:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "Invalid amount : No Negative amount does not give you money ..."
            return ack
        # Create stubs for source and destination banks
        # Source bank channel
        source_channel, success, error = create_channel(from_bank)
        if not success:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = f"Source bank error: {error}"
            return ack
        source_stub = service_pb2_grpc.BankStub(source_channel)

        # Destination bank channel
        dest_bank = request.to_bank
        dest_channel, success, error = create_channel(dest_bank)
        if not success:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = f"Destination bank error: {error}"
            return ack
        dest_stub = service_pb2_grpc.BankStub(dest_channel)

        # Phase 1: Prepare phase with timeout
        debit_req = service_pb2.DebitRequest(
            transaction_id=f"{txn_id}_debit",
            username=from_user,
            password=source_password,
            amount=amount,
            to_bank=dest_bank,
            to_username=request.to_username
        )
        credit_req = service_pb2.CreditRequest(
            transaction_id=f"{txn_id}_credit",
            username=request.to_username,
            amount=amount,
            from_bank=from_bank,
            from_username=from_user
        )
        try :
            prep_debit = source_stub.PrepareDebit(debit_req, timeout=TIMEOUT2PC)
            prep_credit = dest_stub.PrepareCredit(credit_req, timeout=TIMEOUT2PC)
        except grpc.RpcError as e:
            ack = service_pb2.Ack()
            try :
                source_stub.AbortDebit(debit_req)
                dest_stub.AbortCredit(credit_req)
            except grpc.RpcError as e:
                pass
            ack.success = False
            ack.error = f"2PC commit phase timed out or failed with error: {str(e)}"
            return ack


        if not (prep_debit.success and prep_credit.success):
            source_stub.AbortDebit(debit_req)
            dest_stub.AbortCredit(credit_req,)
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "2PC prepare phase failed"
            return ack
        try:
            commit_debit = source_stub.CommitDebit(debit_req, timeout=TIMEOUT2PC)
            commit_credit = dest_stub.CommitCredit(credit_req, timeout=TIMEOUT2PC)
        except grpc.RpcError as e:
            ack = service_pb2.Ack()
            try :
                source_stub.AbortDebit(debit_req)
                dest_stub.AbortCredit(credit_req)
            except grpc.RpcError as e:
                pass
            ack.success = False
            ack.error = f"2PC commit phase timed out or failed with error: {str(e)}"
            return ack
        if commit_debit.success and commit_credit.success:
            with gateway_tx_lock:
                gateway_tx_history.append({
                    'transaction_id': txn_id,
                    'username': from_user,
                    'other_username': request.to_username,
                    'other_bank': dest_bank,
                    'amount': amount,
                    'type': 'transfer'
                })
            ack = service_pb2.Ack()
            ack.success = True
            ack.error = ""
        else:
            if not commit_debit.success:
                source_stub.AbortDebit(debit_req)
            if not commit_credit.success:
                dest_stub.AbortCredit(credit_req)
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "2PC commit phase failed"
        return ack

    def TransactionHistory(self, request, context):
        # Extract user info from token
        token = request.token
        if token not in tokens:
            history = service_pb2.TransactionList()
            history.ack.success = False
            history.ack.error = "Invalid token"
            return history
        
        (username, bank_name) = tokens[token]
        # Forward request (augmented with username and password) to the source bank
        with user_lock:
            if (username, bank_name) not in users:
                history = service_pb2.TransactionList()
                history.ack.success = False
                history.ack.error = "User not logged in"
                return history
            password = users[(username, bank_name)][0]
        auth_req = service_pb2.Auth(username=username, bank_name=bank_name, password=password)
        bank_channel, success, error = create_channel(bank_name)
        if not success:
            history = service_pb2.TransactionList()
            history.ack.success = False
            history.ack.error = error
            return history
        bank_stub = service_pb2_grpc.BankStub(bank_channel)
        return bank_stub.TransactionHistory(auth_req)

## Helper functions ## Dynamic discovery using Consul and bank validation merged into one function

def create_channel(bank_name: str) -> Tuple[Optional[grpc.Channel], bool, str]:
    # Check if bank is registered locally
    if bank_name not in banks:
        return None, False, "Bank not registered"

    # Retrieve the bank certificate from Consul KV store
    index, kv_data = CONSUL_CLIENT.kv.get(f"cert/{bank_name}")
    if not kv_data or "Value" not in kv_data or kv_data["Value"] is None:
        return None, False, f"Certificate for bank '{bank_name}' not found in Consul"
    encoded_cert = kv_data["Value"]
    if isinstance(encoded_cert, str):
        encoded_cert = encoded_cert.encode('utf-8')
    try:
        bank_cert = base64.b64decode(encoded_cert)
    except Exception as e:
        return None, False, f"Error decoding bank certificate: {str(e)}"

    service = CONSUL_CLIENT.agent.services().get(bank_name)
    if service is None:
        return None, False, f"Bank server for '{bank_name}' is down"
    bank_address = f"{service['Address']}:{service['Port']}"

    credentials = grpc.ssl_channel_credentials(root_certificates=bank_cert)
    channel = grpc.secure_channel(bank_address, credentials)
    return channel, True, ""

## Bootstrapping code
def load_config():
    global ADDRESS, CONSUL_CLIENT, TIMEOUT2PC
    with open("../config.json", "r") as f:
        config = json.load(f)
        ADDRESS = config["gateway_addr"]
        TIMEOUT2PC = config["timeout_2pc"]
        consul_address = config["consul_addr"]
    host = consul_address.split(":")[0]
    port = int(consul_address.split(":")[1])
    CONSUL_CLIENT = consul.Consul(host=host, port=port)

def create_cert_key():
    global CERT, KEY, ENCODED_CERT
    os.makedirs("certs", exist_ok=True)
    if (not os.path.exists(f"certs/{NAME}.crt")) or (not os.path.exists(f"certs/{NAME}.key")):
        os.system(f"openssl req -x509 -newkey rsa:4096 -keyout certs/{NAME}.key -out certs/{NAME}.crt -days 365 -nodes -subj '/CN=localhost'")
    with open(f"certs/{NAME}.crt", 'rb') as f:
        CERT = f.read()
    with open(f"certs/{NAME}.key", 'rb') as f:
        KEY = f.read()
    ENCODED_CERT = base64.b64encode(CERT).decode('utf-8')

# Modify create_server to include the interceptors
def create_server():
    credentials = grpc.ssl_server_credentials([(KEY, CERT)])
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[LoggingInterceptor(), JwtInterceptor()]
    )
    service_pb2_grpc.add_GatewayServicer_to_server(GatewayServicer(), server)
    server.add_secure_port(ADDRESS, credentials)
    return server

def signal_handler(sig, frame):
    global server
    print(f"Signal {sig} detected. Exiting...")
    CONSUL_CLIENT.agent.service.deregister("gateway")
    server.stop(0)
    try:
        with open("gateway_data.json", "w") as f:
            data = {
                "banks": {k: (v[0], base64.b64encode(v[1]).decode()) for k, v in banks.items()},
                "users": {f"{u[0]}:{u[1]}": pw for u, (pw, _) in users.items()},
                "tokens": list(tokens.keys()),
                "transaction_ids": list(transaction_ids),
                "gateway_tx_history": gateway_tx_history
            }
            json.dump(data, f,indent=4)
            print("Local data saved to gateway_data.json")
    except Exception as e:
        print(f"Error while storing local data: {e}")
    print("Graceful shutdown complete")
    sys.exit(0)

def load_local_data():
    if os.path.exists("gateway_data.json"):
        with open("gateway_data.json", "r") as f:
            data = json.load(f)
            for k, v in data["banks"].items():
                banks[k] = (v[0], base64.b64decode(v[1]))
            for k, v in data["users"].items():
                user, bank = k.split(":")
                users[(user, bank)] = (v, threading.Lock())
            for token in data["tokens"]:
                try:
                    user_token = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
                    tokens[token] = (user_token["user_id"], user_token["bank"])
                except jwt.PyJWTError:
                    print(f"Skipping invalid token during load")
            for txn_id in data["transaction_ids"]:
                transaction_ids.add(txn_id)
            for txn in data["gateway_tx_history"]:
                gateway_tx_history.append(txn)
        print("Local data loaded from gateway_data.json")
        try:
            os.remove("gateway_data.json")
            print("gateway_data.json deleted successfully.")
        except FileNotFoundError:
            print("gateway_data.json not found.")

# Get address, certificate, and key and register to consul using KV store
def register_to_consul():
    global ADDRESS, CERT, ENCODED_CERT
    # Prepare gateway data
    gateway_data = {
        "address": ADDRESS,
        "certificate": ENCODED_CERT
    }
    
    # Store data in Consul KV store
    result = CONSUL_CLIENT.kv.put('cert/gateway', json.dumps(gateway_data))
    if result:
        print(f"Registered gateway certificate in Consul")
    else:
        print(f"Failed to register gateway certificate in Consul")

    # Also register gateway service with Consul agent
    try:
        host, port = ADDRESS.split(":")
        port = int(port)
        CONSUL_CLIENT.agent.service.register(
            name="gateway",
            address=host,
            port=port,
            tags=["gateway"]
        )
        print(f"Registered gateway service at {ADDRESS} with Consul")
    except Exception as e:
        print(f"Failed to register gateway service: {str(e)}")

def main():
    global server
    load_config()
    create_cert_key()
    print("Got address and certificate")
    load_local_data()
    server = create_server()
    server.start()
    signal.signal(signal.SIGINT, signal_handler)
    print(f"Gateway started at {ADDRESS}")
    register_to_consul()

    server.wait_for_termination()
    

if __name__ == "__main__":
    main()