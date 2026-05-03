from queue import Queue
import os
import sys
import threading
from turtle import back
import uuid

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


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

fraud_grpc_ic = grpc.insecure_channel('fraud_detection:50051')
verification_grpc_ic = grpc.insecure_channel('transaction_verification:50052')
suggestions_grpc_ic = grpc.insecure_channel('suggestions:50053')

fraud_stub    = fraud_detection_grpc.FraudServiceStub(fraud_grpc_ic)
verification_stub = tv_pb2_grpc.TransactionServiceStub(verification_grpc_ic)
suggestions_stub  = s_pb2_grpc.SuggestionsServiceStub(suggestions_grpc_ic)

NODES     = ["transaction", "fraud", "suggestions"]

# -- VECTOR CLOCK API --
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

    # parallel initialisation of transaction, fraud detection and suggestions services
    def transaction_init_worker():
        try:
            vs_req = tv_pb2.InitOrderRequest(
                order_id = order_id,
                user     = user,
                card_number = card_number,
                items    = item_names
            )
            verification_stub.InitOrder(vs_req)
        except Exception as e:
            print(f"ERROR: Transaction InitOrder failed with error: {e}")

    def fraud_init_worker():
        try:
            fd_req = fraud_detection.InitOrderRequest(
                order_id           = order_id,
                user =               user,
                card_number        = card_number,
                order_amount_cents = order_amount_cents
            )
            fraud_stub.InitOrder(fd_req)
        except Exception as e:
            print(f"ERROR: Fraud InitOrder failed with error: {e}")

    def suggestions_init_worker():
        try:
            s_req = s_pb2.InitOrderRequest(
                order_id = order_id,
                items    = item_names
            )
            suggestions_stub.InitOrder(s_req)
        except Exception as e:
            print(f"ERROR: Suggestions InitOrder failed with error: {e}")

    init_workers = [
        threading.Thread(target=transaction_init_worker),
        threading.Thread(target=fraud_init_worker),
        threading.Thread(target=suggestions_init_worker)
    ]

    for worker in init_workers:
        worker.start()

    for worker in init_workers:
        worker.join()

    
    futures = {}
    results = {}
    final_queue = Queue()
    lock = threading.Lock()

    e_clock = zero_clocks()
    stop_scheduler = False
    with ThreadPoolExecutor(max_workers=8) as executor:
        init_clocks = zero_clocks()

        # event (a) - verify items that are not empty
        def verify_items_worker():
            req = tv_pb2.VerifyItemsRequest(
                order_id = order_id,
                vector_clock = init_clocks
            )
            res = verification_stub.VerifyItems(req)
            return res

        # event (b) - mandatory user data is filled
        def verify_user_data_worker():
            req = tv_pb2.VerifyUserDataRequest(
                order_id = order_id,
                vector_clock = init_clocks
            )
            res = verification_stub.VerifyUserData(req)
            return res
        
        # event (c) - verify credit card
        def verify_credit_card_worker(a_clock):
            req = tv_pb2.VerifyCreditCardRequest(
                order_id = order_id,
                vector_clock = a_clock
            )
            res = verification_stub.VerifyCreditCard(req)
            return res
        
        # event (d) - check for user fraud – username consists of numbers
        def check_user_fraud_worker(b_clock):
            req = fraud_detection.CheckUserFraudRequest(
                order_id = order_id,
                vector_clock = b_clock
            )
            res = fraud_stub.CheckUserFraud(req)
            return res
        
        # event (e) – check for credit card fraud
        def check_card_fraud_worker(e_clock):
            req = fraud_detection.CheckCardFraudRequest(
                order_id = order_id,
                vector_clock = e_clock
            )
            res = fraud_stub.CheckCardFraud(req)
            return res
        
        # event (f) - generate suggestions based on items
        def get_suggestions_worker(c_clock):
            req = s_pb2.SuggestionsRequest(
                order_id = order_id,
                vector_clock = c_clock
            )
            res = suggestions_stub.GetSuggestions(req)
            return res

        # triggered after event (a) completes
        def after_a(future_a):
            print("INFO: Event (a) VerifyItems completed with")
            try:
                a_res = future_a.result()
                print("Order ID: ", a_res.order_id)
                print("Vector Clock: ", a_res.vector_clock)
                print("Fail state: ", a_res.failed)
            except Exception as e:
                print(f"ERROR: Event (a) VerifyItems failed with error: {e}")
                results["a"] = f"ERROR: {e}"
                final_queue.put(("a", future_a))
                return
            
            with lock:
                results["a"] = "Order ID: {}\n Vector Clock: {}\n Fail state: {}".format(a_res.order_id, a_res.vector_clock, a_res.failed)
            
            print("INFO: Event (a) VerifyItems callback completed, submitting event (c) VerifyCreditCard.")
            future_c = executor.submit(verify_credit_card_worker, a_res.vector_clock)
            with lock:
                futures["c"] = future_c

            if a_res.failed:
                print("INFO: Event (a) failed, skipping dependent events and putting final future in queue.")
                final_queue.put(("a", future_a))
                return

            if not stop_scheduler:
                future_c.add_done_callback(lambda fut: after_c_or_d("c", fut))
        
        # triggered after event (b) completes
        def after_b(future_b):
            print("INFO: Event (b) VerifyUserData completed with:")
            try:
                b_res = future_b.result()
                print("Order ID: ", b_res.order_id)
                print("Vector Clock: ", b_res.vector_clock)
                print("Fail state: ", b_res.failed)
            except Exception as e:
                print(f"ERROR: Event (b) VerifyUserData failed with error: {e}")
                results["b"] = f"ERROR: {e}"
                final_queue.put(("b", future_b))
                return
            
            with lock:
                results["b"] = "Order ID: {}\n Vector Clock: {}\n Fail state: {}".format(b_res.order_id, b_res.vector_clock, b_res.failed)

            if b_res.failed:
                print("INFO: Event (b) failed, skipping dependent events and putting final future in queue.")
                final_queue.put(("b", future_b))
                return

            future_d = executor.submit(check_user_fraud_worker, b_res.vector_clock)
            with lock:
                futures["d"] = future_d

            if not stop_scheduler:
                future_d.add_done_callback(lambda fut: after_c_or_d("d", fut))

        # triggered after event (c) or (d) completes
        def after_c_or_d(name, future):
            try:
                res = future.result()
                print("Order ID: ", res.order_id)
                print("Vector Clock: ", res.vector_clock)
                print("Fail state: ", res.failed)
            except Exception as e:
                print(f"ERROR: Event (c) or (d) failed with error: {e}")
                with lock:
                    results[name] = f"ERROR: {e}"
                final_queue.put((name, future))
                return

            start_e = False
            with lock:
                results[name] = "Order ID: {}\n Vector Clock: {}\n Fail state: {}".format(res.order_id, res.vector_clock, res.failed)

                if res.failed:
                    print(f"INFO: Event ({name}) failed, skipping dependent events and putting final future in queue.")
                    final_queue.put((name, future))
                    return
                
                if ("c" in results and 
                    "d" in results and 
                    not results["c"].startswith("ERROR") and 
                    not results["d"].startswith("ERROR") and 
                    "e" not in futures):
                    start_e = True
                else:
                    merge_clock(e_clock, res.vector_clock)
            
            if start_e:
                future_e = executor.submit(check_card_fraud_worker, e_clock)
                if not stop_scheduler:
                    future_e.add_done_callback(after_e)

        # triggered after event (f) completes
        def after_f(future_f):
            print("INFO: Event (f) GetSuggestions completed with:")
            try:
                f_res = future_f.result()
                print("Order ID: ", f_res.order_id)
                print("Vector Clock: ", f_res.vector_clock)
                print("Fail state: ", f_res.failed)
                print("Suggested Books: ", f_res.books)
            except Exception as e:
                print(f"ERROR: Event (f) GetSuggestions failed with error: {e}")
                with lock:
                    results["f"] = f"ERROR: {e}"
                final_queue.put(("f", future_f))
                return
            
            with lock:
                results["f"] = "Order ID: {}\n Vector Clock: {}\n Fail state: {}\n Suggested Books: {}".format(
                    f_res.order_id, f_res.vector_clock, f_res.failed, [book.title for book in f_res.books]
                )

            if f_res.failed:
                print("INFO: Event (f) GetSuggestions failed, skipping enqueue, putting final future in queue.")
                final_queue.put(("f", future_f))
                return
            
            print("INFO: Event (f) GetSuggestions callback completed, enqueuing order.")
            try:
                queue_response = enqueue_order(order_id, user, queue_items)
                if not queue_response.ok:
                    raise Exception(queue_response.message)
            except Exception as e:
                print(f"ERROR: Enqueue order failed with error: {e}")
                with lock:
                    results["enqueue"] = f"ERROR: {e}"
                final_queue.put(("enqueue", None))
                return
            
            with lock:
                results["enqueue"] = "Order enqueued successfully."
            final_queue.put(("f", future_f))

        # triggered after event (e) completes
        def after_e(future_e):
            print("INFO: Event (e) CheckCardFraud completed with:")
            try:
                e_res = future_e.result()
                print("Order ID: ", e_res.order_id)
                print("Vector Clock: ", e_res.vector_clock)
                print("Fail state: ", e_res.failed)
            except Exception as e:
                print(f"ERROR: Event (e) CheckCardFraud failed with error: {e}")
                results["e"] = f"ERROR: {e}"
                final_queue.put(("e", future_e))
                return
            
            with lock:
                results["e"] = "Order ID: {}\n Vector Clock: {}\n Fail state: {}".format(e_res.order_id, e_res.vector_clock, e_res.failed)

            if e_res.failed:
                print("INFO: Event (e) CheckCardFraud failed, skipping suggestions and enqueue, putting final future in queue.")
                final_queue.put(("e", future_e))
                return

            print("INFO: Event (e) CheckCardFraud callback completed.")
            future_f = executor.submit(get_suggestions_worker, e_res.vector_clock)
            with lock:
                futures["f"] = future_f
            if not stop_scheduler:
                future_f.add_done_callback(after_f)

        future_a = executor.submit(verify_items_worker)
        future_b = executor.submit(verify_user_data_worker)
        futures["a"] = future_a
        futures["b"] = future_b

        # execute (c) after (a) completes
        future_a.add_done_callback(after_a)
        # execute (d) after (b) completes
        future_b.add_done_callback(after_b)

        finished_name, finished_future = final_queue.get()
        stop_scheduler = True
        print("INFO: Final result future received for event: ", finished_name)
        print("INFO: Final result from the final future:\n", results[finished_name])

    order_status_response = {
        "orderId":        order_id,
        "status":         "Order Approved.",
        "suggestedBooks": []
    }
    return order_status_response, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0')
