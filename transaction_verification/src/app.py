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

NODES     = ["orchestrator", "transaction", "fraud", "suggestions"]
THIS_NODE = "transaction"

ORDERS    = {}
LOCK      = threading.Lock()

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
    def InitOrder(self, request, context):
        print("INFO: Transaction InitOrder request received:")
        print(f"Order ID: {request.order_id}")
        print(f"User:     {request.user}")
        print(f"Items:    {list(request.items)}")

        clock_input = dict(request.vector_clock)

        with LOCK:
            local_clock = merge_clock(zero_clocks(), clock_input)
            local_clock = tick(local_clock, THIS_NODE)

            ORDERS[request.order_id] = {
                "user":         request.user,
                "items":        list(request.items),
                "vector_clock": local_clock
            }
        
        response = tv_pb2.InitOrderResponse(
            ok = True,
            message = f"Transaction service initialised order {request.order_id}"            
        )
        response.vector_clock.update(local_clock)

        print("INFO: Transaction InitOrder response sent.")
        return response

    def VerifyTransaction(self, request, context):
        print("INFO: Transaction verification request received:")
        print(f"Order ID:  {request.order_id}")

        with LOCK:
            if request.order_id not in ORDERS:
                print("ERROR: unknown order_id in transaction service.")

                response = tv_pb2.TransactionResponse(
                    valid = False
                )
                response.vector_clock.update(zero_clocks())
                return response

            order_state = ORDERS[request.order_id]

            local_clock = merge_clock(order_state["vector_clock"],
                                      dict(request.vector_clock))
            local_clock = tick(local_clock, THIS_NODE)
            order_state["vector_clock"] = local_clock

            user  = order_state["user"]
            items = order_state["items"]
        
        print(f"INFO: Cached user:  {user}")
        print(f"INFO: Cached items: {items}")

        valid = bool(items)

        response = tv_pb2.TransactionResponse(
            valid = valid
        )
        response.vector_clock.update(local_clock)

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