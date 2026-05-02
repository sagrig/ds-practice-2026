import sys
import os
import grpc
import threading
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, grpc_path)

import transaction_verification_pb2      as tv_pb2
import transaction_verification_pb2_grpc as tv_pb2_grpc

NODES     = ["orchestrator", "transaction", "fraud", "suggestions", "order_queue"]
THIS_NODE = "transaction"

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

# -- Transaction Service API --
class TransactionService(tv_pb2_grpc.TransactionServiceServicer):
    def __init__(self):
        self.orders = {}
        self.lock = threading.Lock()
        
    def InitOrder(self, request, context):
        print("INFO: Transaction InitOrder request received:")
        print(f"Order ID: {request.order_id}")
        print(f"User:     {request.user}")
        print(f"Items:    {list(request.items)}")

        with self.lock:
            self.orders[request.order_id] = {
                "user":         request.user,
                "items":        list(request.items)
            }
        
        response = tv_pb2.InitOrderResponse(
            ok = True,
            message = f"Transaction service initialised order {request.order_id}"            
        )

        print("INFO: Transaction InitOrder response sent.")
        return response

    def VerifyTransaction(self, request, context):
        print("INFO: Transaction verification request received:")
        print(f"Order ID:  {request.order_id}")

        with self.lock:
            if request.order_id not in self.orders:
                print("ERROR: unknown order_id in transaction service.")

                response = tv_pb2.TransactionResponse(
                    valid = False
                )
                return response

            order_state = self.orders[request.order_id]

            user  = order_state["user"]
            items = order_state["items"]
        
        print(f"INFO: Cached user:  {user}")
        print(f"INFO: Cached items: {items}")

        valid = bool(items)

        response = tv_pb2.TransactionResponse(
            valid = valid
        )

        print("INFO: Transaction verification response:")
        print(f"Valid: {response.valid}")
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    tv_pb2_grpc.add_TransactionServiceServicer_to_server(
        TransactionService(), 
        server
    )

    server.add_insecure_port("[::]:50052")
    server.start()
    print("INFO: Transaction Verification running on 50052")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()