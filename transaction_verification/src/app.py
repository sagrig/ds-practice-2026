import sys
import os
import grpc
import threading
from concurrent import futures
from google.protobuf import empty_pb2

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, grpc_path)

import transaction_verification_pb2      as tv_pb2
import transaction_verification_pb2_grpc as tv_pb2_grpc

# -- Transaction Service API --
class TransactionService(tv_pb2_grpc.TransactionServiceServicer):
    def __init__(self):
        self.NODES = ["transaction", "fraud", "suggestions"]
        self.THIS_NODE = "transaction"
        self.orders = {}
        self.lock = threading.Lock()
        
    def InitOrder(self, request, context):
        print("INFO: Transaction InitOrder request received:")
        print(f"Order ID: {request.order_id}")
        print(f"User:     {request.user}")
        print(f"Credit Card: {request.card_number}")
        print(f"Items:    {list(request.items)}")

        with self.lock:
            self.orders[request.order_id] = {
                "user":         request.user,
                "card_number":  request.card_number,
                "items":        list(request.items),
                "order_clock":  self.zero_clocks()
            }
        
        print("INFO: Transaction InitOrder response sent.")
        return empty_pb2.Empty()
    
    def zero_clocks(self):
        return {node: 0 for node in self.NODES}

    def merge_clocks(self, local, ingress):
        merged = {}
        for node in self.NODES:
            merged[node] = max(local.get(node, 0), ingress.get(node, 0))
        return merged
    
    def tick_clock(self, clock):
        new_clock = dict(clock)
        new_clock[self.THIS_NODE] = new_clock.get(self.THIS_NODE, 0) + 1
        return new_clock

    # event (a) – verify items that are not empty
    def VerifyItems(self, request, context):
        print("INFO: (a) VerifyItems request received:")
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: (a) VerifyItems ingress vector clock: {ingress_clock} for order_id: {request.order_id}")
            order = self.orders.get(request.order_id)
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: (a) VerifyItems updated vector clock: {order['order_clock']} for order_id: {request.order_id}")
            items = order["items"]
            failed = len(items) == 0
            print(f"INFO: (a) VerifyItems fail state: {failed} for order_id: {request.order_id}")
            return  tv_pb2.VerifyItemsResponse(
                order_id = request.order_id,
                failed = failed,
                vector_clock = order["order_clock"]
            )
    
    # event (b) - veify user data is filled
    def VerifyUserData(self, request, context):
        print("INFO: (b) VerifyUserData request received:")
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: (b) VerifyUserData ingress vector clock: {ingress_clock} for order_id: {request.order_id}")
            order = self.orders.get(request.order_id)
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: (b) VerifyUserData updated vector clock: {order['order_clock']} for order_id: {request.order_id}")
            user = order["user"]
            failed = user == ""
            print(f"INFO: (b) VerifyUserData fail state: {failed} for order_id: {request.order_id}")
            return tv_pb2.VerifyUserDataResponse(
                order_id = request.order_id,
                vector_clock = order["order_clock"],
                failed = failed
            )
        
    # event (c) - verify credit card format (doesn't start with 1111)
    def VerifyCreditCard(self, request, context):
        print("INFO: (c) VerifyCreditCard request received:")
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: (c) VerifyCreditCard ingress vector clock: {ingress_clock} for order_id: {request.order_id}")
            order = self.orders.get(request.order_id)
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: (c) VerifyCreditCard updated vector clock: {order['order_clock']} for order_id: {request.order_id}")
            cc = order["card_number"]
            failed = cc == "" or cc.startswith("1111")
            print(f"INFO: (c) VerifyCreditCard fail state: {failed} for order_id: {request.order_id}")
            return tv_pb2.VerifyCreditCardResponse(
                order_id = request.order_id,
                vector_clock = order["order_clock"],
                failed = failed
            )


    # def VerifyTransaction(self, request, context):
    #     print("INFO: Transaction verification request received:")
    #     print(f"Order ID:  {request.order_id}")

    #     with self.lock:
    #         if request.order_id not in self.orders:
    #             print("ERROR: unknown order_id in transaction service.")

    #             response = tv_pb2.TransactionResponse(
    #                 valid = False
    #             )
    #             return response

    #         order_state = self.orders[request.order_id]

    #         user  = order_state["user"]
    #         items = order_state["items"]
        
    #     print(f"INFO: Cached user:  {user}")
    #     print(f"INFO: Cached items: {items}")

    #     valid = bool(items)

    #     response = tv_pb2.TransactionResponse(
    #         valid = valid
    #     )

    #     print("INFO: Transaction verification response:")
    #     print(f"Valid: {response.valid}")
    #     return response


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