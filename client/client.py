
import sys, os, signal, json, base64, hashlib, argparse, uuid
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import protofiles.service_pb2 as service_pb2
import protofiles.service_pb2_grpc as service_pb2_grpc
import grpc
import consul
import threading
import queue
import time

# Global variables
CONSUL_CLIENT = None
GATEWAY_ADDRESS = None
GATEWAY_CERT = None

logged_in = False
TOKEN = None

transaction_queue = queue.Queue()
queue_output = []
txn_queue_lock = threading.Lock()
txn_t = None

# Load configuration and update global consul and gateway settings
def load_config():
    global CONSUL_CLIENT, GATEWAY_ADDRESS, GATEWAY_CERT
    with open("../config.json", "r") as f:
        config = json.load(f)
        host, port = config["consul_addr"].split(":")
    CONSUL_CLIENT = consul.Consul(host=host, port=int(port))
    _, data = CONSUL_CLIENT.kv.get("cert/gateway")
    data = json.loads(data["Value"])
    GATEWAY_ADDRESS = data["address"]
    GATEWAY_CERT = base64.b64decode(data["certificate"])

# Create a new secure channel using grpc for each request
def create_channel():
    global CONSUL_CLIENT, GATEWAY_ADDRESS, GATEWAY_CERT
    index, nodes = CONSUL_CLIENT.health.service("gateway", passing=True)
    if not nodes:
        return None, False, "Gateway service is down"
    credentials = grpc.ssl_channel_credentials(root_certificates=GATEWAY_CERT)
    channel = grpc.secure_channel(GATEWAY_ADDRESS, credentials)
    return channel, True, ""

# Perform login and return token if successful
def perform_login(bank_name, username, password):
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    channel, success, error = create_channel()
    if not success:
        print("Error:", error)
        return None, error
    with channel:
        stub = service_pb2_grpc.GatewayStub(channel)
        request = service_pb2.Auth(username=username, bank_name=bank_name, password=password_hash)
        response = stub.Login(request)
        if response.success:
            print("Logged in successfully.")
            return response.token, ""
        else:
            print("Login failed:", response.error)
            return None, response.error

# Logout using a new channel
def perform_logout(token):
    channel, success, error = create_channel()
    if not success:
        print("Error during logout:", error)
        return
    with channel:
        stub = service_pb2_grpc.GatewayStub(channel)
        logout_request = service_pb2.Token(token=token)
        response = stub.Logout(logout_request)
        if response.success:
            print("Logged out successfully.")
        else:
            print("Logout failed:", response.error)

# Signal handler for graceful termination
def signal_handler(sig, frame):
    global logged_in, TOKEN
    if logged_in:
        print(f"\n{sig} detected. Logging out...")
        perform_logout(TOKEN)
    else:
        print("\nCtrl+C detected. Exiting...")
    if txn_t is not None and txn_t.is_alive():
        txn_t.join(timeout=0.5)
    sys.exit(0)

# Check balance operation using a new channel
def check_balance(token):
    channel, success, error = create_channel()
    if not success:
        print("Error:", error)
        return
    metadata = [("token", token)]
    with channel:
        stub = service_pb2_grpc.GatewayStub(channel)
        request = service_pb2.Token(token=token)
        response = stub.ViewBalance(request, metadata=metadata)
        if response.ack.success:
            print("Your balance is:", response.balance)
        else:
            print("Error:", response.ack.error)

# Initiate a money transfer using a new channel
def initiate_transfer(token):
    
    txn_id = str(uuid.uuid4())
    to_bank = input("Enter destination bank: ")
    to_username = input("Enter destination username: ")
    try:
        amount = int(input("Enter amount to transfer (int): "))
    except ValueError:
        print("Amount must be an integer.")
        return
    print(f"Generated Transaction ID: {txn_id}")
    

    transfer_request = service_pb2.Transfer(
        transaction_id=txn_id,
        token=token,
        to_bank=to_bank,
        to_username=to_username,
        amount=amount
    )

    channel, success, error = create_channel()

    if not success:
        print("Error:", error)
        print("Adding transaction to queue.")
        with txn_queue_lock:
            transaction_queue.put(transfer_request)
        return
    metadata = [("token", token)]

    with channel:
        stub = service_pb2_grpc.GatewayStub(channel)
        response = stub.TransferMoney(transfer_request, metadata=metadata)
        print("Transfer success:", response.success)
        if not response.success:
            print("Error:", response.error)

# Retrieve transaction history using a new channel
def view_transaction_history(token):
    channel, success, error = create_channel()
    if not success:
        print("Error:", error)
        return
    metadata = [("token", token)]
    with channel:
        stub = service_pb2_grpc.GatewayStub(channel)
        txn_history_request = service_pb2.Token(token=token)
        history_response = stub.TransactionHistory(txn_history_request, metadata=metadata)
        if history_response.ack.success:
            if history_response.transactions:
                print("Transaction History:")
                for txn in history_response.transactions:
                    # Updated to use new field names and include type
                    print(f"ID: {txn.transaction_id}, username: {txn.username}, \n"
                          f"other_username: {txn.other_username}, other_bank: {txn.other_bank}, \n"
                          f"amount: {txn.amount}, type: {txn.type}\n\n")
            else:
                print("No transactions found.")
        else:
            print("Error:", history_response.ack.error)

def offline_txn_worker():
    global transaction_queue, txn_queue_lock, queue_output
    while True:
        while not transaction_queue.empty():
            gateway_nodes = CONSUL_CLIENT.health.service("gateway", passing=True)[1]
            is_gateway_available = bool(gateway_nodes)
            if not transaction_queue.empty() and is_gateway_available:
                with txn_queue_lock:
                    transfer_request = transaction_queue.get()
                channel, success, error = create_channel()
                if not success:
                    print("Error:", error)
                    continue
                metadata = [("token", transfer_request.token)]
                with channel:
                    try: 
                        stub = service_pb2_grpc.GatewayStub(channel)
                        response = stub.TransferMoney(transfer_request, metadata=metadata)
                    except grpc.RpcError as e:
                        if "token expired" in str(e).lower():
                            with txn_queue_lock:
                                queue_output.append(f"Transaction {transfer_request.transaction_id} skipped: token expired")
                            continue
                with txn_queue_lock:
                    if response.success:
                            queue_output.append(
                                f"Transaction {transfer_request.transaction_id} processed successfully by the gateway. \n"
                                f"Details: Destination Bank({transfer_request.to_bank}), Destination Username({transfer_request.to_username}), Amount({transfer_request.amount}). \n"
                                "The gateway has recorded and processed this transaction successfully.\n\n"
                            )
                    else:
                        queue_output.append(
                            f"Transaction {transfer_request.transaction_id} failed to process. \n"
                            f"Details: Destination Bank({transfer_request.to_bank}), Destination Username({transfer_request.to_username}), Amount({transfer_request.amount}). \n"
                            f"Encountered Error: {response.error}. \n"
                            "Please check the gateway logs or try resubmitting the transaction later.\n\n"
                        )
            else:
                time.sleep(0.2)
        else :
            time.sleep(0.2)

# Main driver function
def main():
    global logged_in, TOKEN, txn_t
    load_config()

    txn_t = threading.Thread(target=offline_txn_worker, daemon=True)
    txn_t.start()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Client utility")
    parser.add_argument("bank_name", nargs="?", help="Bank name")
    parser.add_argument("username", nargs="?", help="Username")
    parser.add_argument("password", nargs="?", help="Password")
    args = parser.parse_args()

    # Try login using provided arguments
    if args.bank_name and args.username and args.password:
        TOKEN, err = perform_login(args.bank_name, args.username, args.password)
        if TOKEN:
            logged_in = True

    # If not logged in using CLI arguments, ask for details
    while not logged_in:
        try:
            bank_name = input("Enter bank name: ")
            username = input("Enter username: ")
            password_input = input("Enter password: ")
            TOKEN, err = perform_login(bank_name, username, password_input)
            if TOKEN:
                logged_in = True
        except EOFError:
            break

    # Main loop once logged in
    while logged_in:
        print("\nOptions:")
        print("1. Check balance")
        print("2. Initiate Transfer")
        print("3. View Transaction History")
        print("4. Pass.. view offline transaction stauses")
        print("5. Logout")
        try:
            choice = int(input("Enter choice: "))
        except ValueError:
            print("Invalid choice. Please enter a number.")
            continue

        if choice == 1:
            check_balance(TOKEN)
        elif choice == 2:
            initiate_transfer(TOKEN)
        elif choice == 3:
            view_transaction_history(TOKEN)
        elif choice == 4:
            global queue_output, txn_queue_lock
            with txn_queue_lock:
                for item in queue_output:
                    print(item)
                queue_output = []
        elif choice == 5:
            perform_logout(TOKEN)
            logged_in = False
        else:
            print("Invalid choice. Please enter a valid number.")

if __name__ == "__main__":
    main()
