import os
import sys
import threading
import uuid

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

# fraud detection gRPC
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

# order queue gRPC
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))
sys.path.insert(0, order_queue_grpc_path)
import order_queue_pb2      as order_queue_pb2
import order_queue_pb2_grpc as order_queue_pb2_grpc

import grpc

# -- VECTOR CLOCK API --
NODES = ["orchestrator", "transaction", "fraud", "suggestions", "order_queue", "order_executor"]
THIS_NODE = "orchestrator"

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
def init_transaction(order_id, user, items):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub    = tv_pb2_grpc.TransactionServiceStub(channel)
        request = tv_pb2.InitOrderRequest(
            order_id = order_id,
            user     = user,
            items    = items
        )
        response = stub.InitOrder(request)
    return response

def init_suggestions(order_id, items):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub    = s_pb2_grpc.SuggestionsServiceStub(channel)
        request = s_pb2.InitOrderRequest(
            order_id = order_id,
            items    = items
        )
        response = stub.InitOrder(request)
    return response

def init_fraud(order_id, card_number, order_amount_cents):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub    = fraud_detection_grpc.FraudServiceStub(channel)
        request = fraud_detection.InitOrderRequest(
            order_id           = order_id,
            card_number        = card_number,
            order_amount_cents = order_amount_cents
        )
        response = stub.InitOrder(request)
    return response

# -- Actual RPC calls --
def verify_transaction(order_id):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub    = tv_pb2_grpc.TransactionServiceStub(channel)
        request = tv_pb2.TransactionRequest(
            order_id = order_id
        )

        response = stub.VerifyTransaction(request)
    return response

def get_suggestions(order_id):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub     = s_pb2_grpc.SuggestionsServiceStub(channel)

        request  = s_pb2.SuggestionsRequest(
            order_id = order_id
        )

        response = stub.GetSuggestions(request)

    books = []
    for b in response.books:
        books.append({
            "bookId": b.bookId,
            "title":  b.title,
            "author": b.author
        })

    return {
        "books":        books
    }

def check_fraud(order_id):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudServiceStub(channel)

        request = fraud_detection.FraudRequest(
            order_id = order_id
        )
        response = stub.CheckFraud(request)
    return {
        "is_fraud":     response.is_fraud
    }


def enqueue_order(order_id, user, items):
    with grpc.insecure_channel('order_queue:50054') as channel:
        stub = order_queue_pb2_grpc.OrderQueueServiceStub(channel)
        request = order_queue_pb2.EnqueueRequest(order_id=order_id, user=user)
        for item in items:
            request.items.add(
                title=item["title"],
                quantity=item["quantity"],
            )
        response = stub.Enqueue(request)
    return response

# -- FLASK --
from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': '*'}})

@app.route('/', methods=['GET'])
def index():
    return "INFO: Orchestrator runs successfully!"

@app.route('/checkout', methods=['POST'])
def checkout():
    print("INFO: Orchestrator received /checkout request.")

    request_data = request.get_json()

    if not request_data:
        norequest_error_response = {
            "code":    "400",
            "message": "Invalid or empty JSON body."
        }
        return norequest_error_response, 400

    if not request_data.get("items"):
        items_error_response = {
            "code":    "400",
            "message": "Order must contain at least one item."
        }
        return items_error_response, 400

    if not request_data.get("termsAndConditionsAccepted"):
        terms_error_response = {
            "code":    "400",
            "message": "Terms and Conditions must be accepted."
        }
        return terms_error_response, 400

    print("INFO: Orchestrator validated items and terms fields.")

    # extracting order info
    order_id           = str(uuid.uuid4())
    item_names         = [item.get("name", "") for item in request_data.get("items", [])]
    queue_items        = [
        {
            "title": item.get("name", ""),
            "quantity": int(item.get("quantity", 1) or 1),
        }
        for item in request_data.get("items", [])
        if item.get("name")
    ]
    user               = request_data.get("user", {}).get("name", "")
    card_number        = request_data.get("creditCard", {}).get("number", "")
    order_amount_cents = sum(item["quantity"] for item in queue_items) * 100
    invalid_disc       = (request_data.get("discountCode") == "INVALID")

    print(f"INFO: Generated Order ID: {order_id}")

    # stage 1: a || b
    stage1_results = {}
    stage1_errors  = []

    def transaction_init_worker():
        try:
            stage1_results["a"] = init_transaction(order_id, user, item_names)
        except Exception as e:
            stage1_errors.append(f"ERROR: Event a failed: {e}")

    def fraud_init_worker():
        try:
            stage1_results["b"] = init_fraud(order_id, card_number, order_amount_cents)
        except Exception as e:
            stage1_errors.append(f"ERROR: Event b failed: {e}")

    # a = transaction_init
    a_thread = threading.Thread(target=transaction_init_worker)
    # b = fraud_init 
    b_thread = threading.Thread(target=fraud_init_worker)

    a_thread.start()
    b_thread.start()

    a_thread.join()
    b_thread.join()

    if stage1_errors:
        return {
            "code":    "500",
            "message": "Stage 1 failed.",
            "details": stage1_errors
        }, 500

    if "a" not in stage1_results or "b" not in stage1_results:
        return {
            "code":    "500",
            "message": "Stage 1 failed."
        }, 500

    if not stage1_results["a"].ok or not stage1_results["b"].ok:
        return {
            "orderId":        order_id,
            "status":         "Order Rejected!",
            "suggestedBooks": []
        }, 200

    order_status_response = {
        "orderId":        order_id,
        "status":         "Order Approved.",
        "suggestedBooks": ["test"]
    }

    # c || d (both after a)
    stage2_results = {}
    stage2_errors  = []

    def suggestions_init_worker():
        try:
            stage2_results["c"] = init_suggestions(order_id, item_names)
        except Exception as e:
            stage2_errors.append(f"ERROR: Event c failed: {e}")

    def transaction_verify_worker():
        try:
            stage2_results["d"] = verify_transaction(order_id)
        except Exception as e:
            stage2_errors.append(f"ERROR: Event d failed: {e}")

    # c = suggestions_init
    c_thread = threading.Thread(target=suggestions_init_worker)
    # d = transaction_verify
    d_thread = threading.Thread(target=transaction_verify_worker)

    c_thread.start()
    d_thread.start()

    c_thread.join()
    d_thread.join()

    if stage2_errors:
        return {
            "code":    "500",
            "message": "Stage 2 failed.",
            "details": stage2_errors
        }, 500

    if "c" not in stage2_results or "d" not in stage2_results:
        return {
            "code":    "500",
            "message": "Stage 2 failed."
        }, 500

    if not stage2_results["c"].ok:
        return {
            "code":    "500",
            "message": "Suggestions Initialisation event failed."
        }, 500

    tr_valid = stage2_results["d"].valid

    if not tr_valid or invalid_disc:
        return {
            "orderId":        order_id,
            "status":         "Order Rejected!",
            "suggestedBooks": []
        }, 200

    # e (after b and d)

    # e = check_fraud
    try:
        e_result = check_fraud(order_id)
    except Exception as e:
        return {
            "code":    "500",
            "message": "Event e failed.",
            "details": str(e)
        }, 500

    is_fraud = e_result["is_fraud"]
    if is_fraud:
        return {
            "orderId":        order_id,
            "status":         "Order Rejected!",
            "suggestedBooks": []
        }, 200

    # f (after c and e)

    # f = get_suggestions
    try:
        f_result = get_suggestions(order_id)
    except Exception as e:
        return {
            "code":    "500",
            "message": "Event f failed.",
            "details": str(e)
        }, 500
    try:
        queue_response = enqueue_order(order_id, user, queue_items)
    except Exception as e:
        return {
            "code":    "500",
            "message": "Queue enqueue failed.",
            "details": str(e)
        }, 500
    if not queue_response.ok:
        queue_response_fail = {
            "code": "500",
            "message": "Order could not be enqueued.",
            "details": queue_response.message
        }
        return queue_response_fail, 500    
    order_status_response = {
        "orderId":        order_id,
        "status":         "Order Approved.",
        "suggestedBooks": f_result["books"]
    }

    return order_status_response, 200


if __name__ == '__main__':
    app.run(host='0.0.0.0')
