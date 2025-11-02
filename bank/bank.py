# Imports
import sys, os, signal
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
import argparse
import threading
import hashlib
import logging

# Constants
CONSUL_CLIENT = None
GATEWAY_ADDRESS = None
GATEWAY_CERT  = None

ADDRESS = None
PORT = None

NAME = None
CERT = None
ENCODED_CERT = None
KEY = None

## Data structures
client_data = dict() # user_id -> (password, bank_name, balance, lock)

prepare_transaction = dict()   # transaction_id -> { 'username': str, 'amount': int, 'type': 'debit'/'credit' }
history_lock = threading.Lock()
transaction_history = dict() # username -> [ { 'transaction_id': str, 'username': str, 'other_username': str, 'other_bank': str, 'amount': int, 'type': 'debit'/'credit' } ]

# Logging interceptor
class LoggingInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        logger_name = f"{NAME}_interceptor"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler(f"{NAME}_interceptor.log")
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

def authenticate_user(user_id, password) -> Tuple[bool, str]:
    if user_id in client_data:
        with client_data[user_id]["lock"]:
            if client_data[user_id]["password"] == password:
                return True, ""
            else:
                return False, "Incorrect password"
    else:
        return False, "User not found"


# Servicer
class BankServicer(service_pb2_grpc.BankServicer):
    def Authenticate(self, request, context):

        user_id = request.username
        password = request.password
        response = service_pb2.Ack()
        success, error = authenticate_user(user_id, password)
        response.success = success
        response.error = error
        return response

    def ViewBalance(self, request, context):
        user_id = request.username
        password = request.password

        success, error = authenticate_user(user_id, password)
        if not success:
            response = service_pb2.Balance()
            response.ack.success = False
            response.ack.error = error
            return response
        
        response = service_pb2.Balance()
        with client_data[user_id]["lock"]:
            response.balance = client_data[user_id]["balance"]
        response.ack.success = True
        response.ack.error = ""
        return response

    def PrepareDebit(self, request, context):
        txn_id = request.transaction_id

        if txn_id in prepare_transaction:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "Transaction ID already exists"
            return ack
        if request.username not in client_data:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "User not found"
            return ack

        # Validate user and check sufficient funds
        user_id = request.username
        password = request.password
        amount = request.amount
        # Validate amount is positive
        if amount <= 0:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "Amount should be positive"
            return ack

        # Authenticate user
        valid, error = authenticate_user(user_id, password)
        if not valid:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = error
            return ack
        with client_data[user_id]["lock"]:
            if client_data[user_id]["balance"] < amount:
                ack = service_pb2.Ack()
                ack.success = False
                ack.error = "Insufficient funds"
                return ack
        # Debit funds
        with client_data[user_id]["lock"]:
            client_data[user_id]["balance"] -= amount
        # Reserve funds
        prepare_transaction[request.transaction_id] = {'username': user_id, 'amount': amount, 'type': 'debit'}
        ack = service_pb2.Ack()
        ack.success = True
        return ack

    def PrepareCredit(self, request, context):

        if request.transaction_id in prepare_transaction:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "Transaction ID already exists"
            return ack
        
        if request.username not in client_data:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "User not found"
            return ack
        # For credit, simply record the intent
        prepare_transaction[request.transaction_id] = {'username': request.username, 'amount': request.amount, 'type': 'credit'}
        ack = service_pb2.Ack()
        ack.success = True
        return ack

    def CommitDebit(self, request, context):
        user_id = request.username
        if request.username not in client_data:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "User not found"
            return ack
        amount = request.amount
        txn = prepare_transaction.pop(request.transaction_id, None)
        if not txn or txn.get('type') != 'debit':
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "No matching debit transaction"
            return ack
        # Record transaction with new field names and type 'debit'
        with client_data[user_id]["lock"]:
            transaction_history.setdefault(user_id, []).append({
                'transaction_id': request.transaction_id,
                'username': user_id,
                'other_username': request.to_username,
                'other_bank': request.to_bank,
                'amount': amount,
                'type': 'debit'
            })
        ack = service_pb2.Ack()
        ack.success = True
        return ack

    def CommitCredit(self, request, context):
        user_id = request.username
        if request.username not in client_data:
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "User not found"
            return ack
        amount = request.amount
        txn = prepare_transaction.pop(request.transaction_id, None)
        if not txn or txn.get('type') != 'credit':
            ack = service_pb2.Ack()
            ack.success = False
            ack.error = "No matching credit transaction"
            return ack
        with client_data[user_id]["lock"]:
            client_data[user_id]["balance"] += amount
            transaction_history.setdefault(user_id, []).append({
                'transaction_id': request.transaction_id,
                'username': user_id,
                'other_username': request.from_username,  # using from_username as other party
                'other_bank': request.from_bank,
                'amount': amount,
                'type': 'credit'
            })
        ack = service_pb2.Ack()
        ack.success = True
        return ack

    def AbortDebit(self, request, context):
        # First, try to remove a reserved (but not yet committed) debit transaction.
        txn = prepare_transaction.pop(request.transaction_id, None)
        if txn and txn.get('type') == 'debit':
            with client_data[txn['username']]["lock"]:
                client_data[txn['username']]["balance"] += txn['amount']
            ack = service_pb2.Ack()
            ack.success = True
            return ack

        user_id = request.username
        refund_done = False
        with client_data[user_id]["lock"]:
            user_txns = transaction_history.get(user_id, [])
            for i, t in enumerate(user_txns):
                if t["transaction_id"] == request.transaction_id and t["type"] == "debit":
                    client_data[user_id]["balance"] += t["amount"]
                    del user_txns[i]
                    refund_done = True
                    break
        ack = service_pb2.Ack()
        if refund_done:
            ack.success = True
        else:
            ack.success = False
            ack.error = "No matching debit transaction found to abort."
        return ack

    def AbortCredit(self, request, context):
        # First, try to remove a reserved (but not yet committed) credit transaction.
        txn = prepare_transaction.pop(request.transaction_id, None)
        if txn and txn.get('type') == 'credit':
            ack = service_pb2.Ack()
            ack.success = True
            return ack

        user_id = request.username
        reversal_done = False
        with client_data[user_id]["lock"]:
            user_txns = transaction_history.get(user_id, [])
            for i, t in enumerate(user_txns):
                if t["transaction_id"] == request.transaction_id and t["type"] == "credit":
                    if client_data[user_id]["balance"] < t["amount"]:
                        ack = service_pb2.Ack()
                        ack.success = False
                        ack.error = "Insufficient funds to reverse credit transaction"
                        return ack
                    client_data[user_id]["balance"] -= t["amount"]
                    del user_txns[i]
                    reversal_done = True
                    break
        ack = service_pb2.Ack()
        if reversal_done:
            ack.success = True
        else:
            ack.success = False
            ack.error = "No matching credit transaction found to abort."
        return ack

    def TransactionHistory(self, request, context):
        # Authenticate user (using same logic as Authenticate)
        user_id = request.username
        password = request.password
        valid, error = authenticate_user(user_id, password)
        history = service_pb2.TransactionList()
        if not valid:
            history.ack.success = False
            history.ack.error = error
            return history
        with client_data[user_id]["lock"]:
            user_txns = transaction_history.get(user_id, [])
        # Append transactions using the new field names
        for t in user_txns:
            tx = history.transactions.add()
            tx.transaction_id = t["transaction_id"]
            tx.username = t["username"]
            tx.other_username = t["other_username"]
            tx.other_bank = t["other_bank"]
            tx.amount = t["amount"]
            tx.type = t["type"]
        history.ack.success = True
        history.ack.error = ""
        return history


## Helper functions
def create_channel() -> grpc.Channel:
    credentials = grpc.ssl_channel_credentials(root_certificates=GATEWAY_CERT)
    channel = grpc.secure_channel(GATEWAY_ADDRESS, credentials)
    return channel

## Bootstrapping code
def load_config():
    global CONSUL_CLIENT, GATEWAY_ADDRESS, GATEWAY_CERT
    with open("../config.json", "r") as f:
        config = json.load(f)
        host, port = config["consul_addr"].split(":")
    
    if NAME is None:
        print("Error: Bank name not specified. Use --name parameter.")
        sys.exit(1)

    with open("./user.json", "r") as f:
        all_data = json.load(f)
        if NAME not in all_data:
            print(f"Error: Bank '{NAME}' not found in user.json")
            sys.exit(1)
        data = all_data[NAME]

    for user in data:
        client_data[user["username"]] = dict()
        hashed_password = hashlib.sha256(user["password"].encode()).hexdigest()
        client_data[user["username"]]["password"] = hashed_password
        client_data[user["username"]]["balance"] = user["balance"]
        client_data[user["username"]]["lock"] = threading.Lock()

    CONSUL_CLIENT = consul.Consul(host=host, port=int(port))
    _, data = CONSUL_CLIENT.kv.get("cert/gateway")
    data = json.loads(data["Value"])    
    GATEWAY_ADDRESS = data["address"]
    GATEWAY_CERT = base64.b64decode(data["certificate"])

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

def create_server():
    global PORT, ADDRESS
    credentials = grpc.ssl_server_credentials([(KEY, CERT)])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), interceptors=[LoggingInterceptor()])
    service_pb2_grpc.add_BankServicer_to_server(BankServicer(), server)
    PORT = server.add_secure_port(f"[::]:0", credentials)
    ADDRESS = f"localhost:{PORT}"
    return server

def register_to_consul():
    # Register the bank service with Consul
    CONSUL_CLIENT.agent.service.register(
        name=NAME,
        address=ADDRESS.split(':')[0],
        port=PORT,
        tags=["bank"]
    )
    # Update the KV store with the bank certificate under the key cert/{bank}
    CONSUL_CLIENT.kv.put(f"cert/{NAME}", ENCODED_CERT)
    signal.signal(signal.SIGINT, signal_handler)
    print(f"Registered bank - {NAME} with consul at {ADDRESS.split(':')[0]}:{PORT}")

def register_to_gateway():
    channel = create_channel()
    stub = service_pb2_grpc.GatewayStub(channel)
    response = stub.RegisterBank(service_pb2.BankName(bank_name=NAME, bank_address=ADDRESS, bank_cert=ENCODED_CERT))
    print(f"Registered bank - {NAME} with gateway")

## Graceful exit
def signal_handler(sig, frame):
    print("\nCtrl+C detected. Exiting...")
    CONSUL_CLIENT.agent.service.deregister(NAME)
    with open("./user.json", "r") as f:
        data = json.load(f)

    for record in data[NAME]:
        username = record["username"]
        record["balance"] = client_data[username]["balance"]

    with open("./user.json", "w") as f:
        json.dump(data, f, indent=4)
    sys.exit(0)

def main():
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--name", type=str, help="Name of the gateway")
    global NAME
    args = argParser.parse_args()
    NAME = args.name
    if NAME is None:
        print("Error: Bank name must be specified with --name")
        sys.exit(1)

    load_config()
    create_cert_key()
    print("Got address and certificate")
    server = create_server()
    server.start()

    register_to_consul()
    register_to_gateway()
    server.wait_for_termination()

    pass

if __name__ == "__main__":
    main()