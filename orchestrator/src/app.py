import os
import sys
import threading
import uuid

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

# fraud detextion gRPC
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2      as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

# transaction verification gRPC
tv_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, tv_grpc_path)
import transaction_verification_pb2      as tv_pb2
import transaction_verification_pb2_grpc as tv_pb2_grpc

# suggestions gRPC
s_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, s_grpc_path)
import suggestions_pb2      as s_pb2
import suggestions_pb2_grpc as s_pb2_grpc

import grpc

# -- VECTOR CLOCK API --
NODES = ["orchestrator", "transaction", "fraud", "suggestions"]

def zero_clocks():
    return {node: 0 for node in NODES}

def tick(clock, node):
    new_clock = dict(clock)
    new_clock[node] = new_clock.get(node, 0) + 1
    return new_clock

def merge_clock(a, b):
    merged = {}
    for node in NODES:
        merged[node] = max(a.get(node, 0), b.get(node, 0))
    return merged

# -- Initial RPC calls --
def init_transaction(order_id, user, items, vector_clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub    = tv_pb2_grpc.TransactionServiceStub(channel)
        request = tv_pb2.InitOrderRequest(
            order_id = order_id,
            user     = user,
            items    = items
        )
        request.vector_clock.update(vector_clock)

        response = stub.InitOrder(request)
    return response

def init_suggestions(order_id, items, vector_clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub    = s_pb2_grpc.SuggestionsServiceStub(channel)
        request = s_pb2.InitOrderRequest(
            order_id = order_id,
            items    = items
        )
        request.vector_clock.update(vector_clock)

        response = stub.InitOrder(request)
    return response

def init_fraud(order_id, card_number, order_amount_cents, vector_clock):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub    = fraud_detection_grpc.FraudServiceStub(channel)
        request = fraud_detection.InitOrderRequest(
            order_id           = order_id,
            card_number        = card_number, 
            order_amount_cents = order_amount_cents
        )
        request.vector_clock.update(vector_clock)

        response = stub.InitOrder(request)
    return response

# -- Actual RPC calls --
def verify_transaction(order_id, vector_clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub    = tv_pb2_grpc.TransactionServiceStub(channel)
        request = tv_pb2.TransactionRequest(
            order_id     = order_id
        )   
        request.vector_clock.update(vector_clock)

        response = stub.VerifyTransaction(request)
    return response

def get_suggestions(order_id, vector_clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub     = s_pb2_grpc.SuggestionsServiceStub(channel)

        request  = s_pb2.SuggestionsRequest(
            order_id = order_id
        )
        request.vector_clock.update(vector_clock)

        response = stub.GetSuggestions(request)

    books = []
    for b in response.books:
        books.append({
            "bookId": b.bookId,
            "title":  b.title,
            "author": b.author
        })

    return {
        "books":        books,
        "vector_clock": dict(response.vector_clock)
    }

def check_fraud(order_id, vector_clock):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudServiceStub(channel)

        request = fraud_detection.FraudRequest(
            order_id = order_id
        )
        request.vector_clock.update(vector_clock)

        response = stub.CheckFraud(request)
    return {
        "is_fraud":     response.is_fraud,
        "vector_clock": dict(response.vector_clock)
    }

# -- FLASK -- 
from flask import Flask, request
from flask_cors import CORS

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.
@app.route('/', methods=['GET'])
def index():
    return "Orchestrator runs successfully!"

@app.route('/checkout', methods=['POST'])
def checkout():
    print("Orchestrator received /checkout request.")
    
    request_data = request.get_json()

    if not request_data:
        norequest_error_response = {
            "code": "400",
            "message": "Invalid or empty JSON body."            
        }
        return norequest_error_response, 400

    if not request_data.get("items"):
        items_error_response = {
            "code": "400",
            "message": "Order must contain at least one item."
        }
        return items_error_response, 400
    
    if not request_data.get("termsAndConditionsAccepted"):
        terms_error_response = {
            "code": "400",
            "message": "Terms and Conditions must be accepted."
        }
        return terms_error_response, 400

    print("INFO: Orchestrator validated items and terms fields.")
    print("INFO: Orchestrator spawns worker threads.")

    # extracting order info
    order_id           = str(uuid.uuid4())
    items              = [item.get("name", "") for item in request_data.get("items", [])]
    user               = request_data.get("user", {}).get("name", "")
    card_number        = request_data.get("creditCard", {}).get("number", "")
    order_amount_cents = len(request_data.get("items", [])) * 100

    # initial orchestrator event
    global_clock = zero_clocks();
    global_clock = tick(global_clock, "orchestrator")

    print(f"INFO: Generated Order ID: {order_id}")
    print("INFO: Orchestrator starts initialisation of all services.")

    init_results = {}
    init_errors  = []

    def transaction_init_worker():
        try:
            init_results["transaction"] = init_transaction(order_id, 
                                                           user, 
                                                           items, 
                                                           global_clock)
        except Exception as e:
            init_errors.append(f"ERROR: Transaction init failed: {e}")
    
    def suggestions_init_worker():
        try:
            init_results["suggestions"] = init_suggestions(order_id, 
                                                           items, 
                                                           global_clock)
        except Exception as e:
            init_errors.append(f"ERROR: Suggestions init failed: {e}")

    def fraud_init_worker():
        try:
            init_results["fraud"]       = init_fraud(order_id, 
                                                     card_number,
                                                     order_amount_cents,
                                                     global_clock)
        except Exception as e:
            init_errors.append(f"ERROR: Fraud init failed: {e}")

    tr_init_thread = threading.Thread(target=transaction_init_worker)
    sg_init_thread = threading.Thread(target=suggestions_init_worker)
    fr_init_thread = threading.Thread(target=fraud_init_worker)

    tr_init_thread.start()
    sg_init_thread.start()
    fr_init_thread.start()

    tr_init_thread.join()
    sg_init_thread.join()
    fr_init_thread.join()

    if init_errors:
        print("ERROR: initialisation process has failed.")
        init_error_response = {
            "code":    "500",
            "message": "initialisation failed",
            "details": init_errors
        }
        return init_error_response, 500

    for response in init_results.values():
        global_clock = merge_clock(global_clock, dict(response.vector_clock))

    print("INFO: initialisation process has successfully completed.")
    print("INFO: orchestrator starts actual verification.")

    run_results = {}
    run_errors  = []

    def transaction_worker():
        try:
            run_results["transaction"] = verify_transaction(order_id,
                                                            global_clock)
        except Exception as e:
            run_errors.append(f"ERROR: transaction verification worker failed: {e}")
    
    def fraud_worker():
        try:
            run_results["fraud"] = check_fraud(order_id,
                                               global_clock)
        except Exception as e:
            run_errors.append(f"ERROR: fraud checking worker failed: {e}")
    
    tr_thread = threading.Thread(target=transaction_worker)
    fr_thread = threading.Thread(target=fraud_worker)

    tr_thread.start()
    fr_thread.start()    

    tr_thread.join()
    fr_thread.join()    

    if run_errors:
        print("ERROR: one or more of gRPC workers failed.")
        run_error_response = {
            "code":    "500",
            "message": "One or more gRPC workers failed.",
            "details": run_errors
        }
        return run_error_response, 500
    
    tr_response  = run_results["transaction"]
    fr_response  = run_results["fraud"]

    global_clock = merge_clock(global_clock, dict(tr_response.vector_clock))
    global_clock = merge_clock(global_clock, fr_response["vector_clock"])

    is_fraud     = fr_response["is_fraud"]
    tr_valid     = tr_response.valid
    invalid_disc = (request_data.get("discountCode") == "INVALID")

    if is_fraud or not tr_valid or invalid_disc:
        status       = "Order Rejected!"
        sug_books    = []
    else:
        sugg_resp    = get_suggestions(order_id, global_clock)
        global_clock = merge_clock(global_clock, sugg_resp["vector_clock"])
        status       = "Order Approved."
        sug_books    = sugg_resp["books"]
    
    order_status_response = {
        "orderId":        order_id,
        "status":         status,
        "suggestedBooks": sug_books
    }

    return order_status_response, 200


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
