import sys
import os
from urllib import request
import grpc
import threading
from concurrent import futures

from google.protobuf import empty_pb2


FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, grpc_path)

import suggestions_pb2 as s_pb2
import suggestions_pb2_grpc as s_pb2_grpc


# -- Suggestions Service API --
class SuggestionsService(s_pb2_grpc.SuggestionsServiceServicer):
    def __init__(self):
        self.NODES = ["transaction", "fraud", "suggestions"]
        self.THIS_NODE = "suggestions"
        self.orders = {}
        self.lock   = threading.Lock()

    def InitOrder(self, request, context):
        print("INFO: Suggestions InitOrder request received:")
        print(f"Order ID: {request.order_id}")
        print(f"Items:    {list(request.items)}")


        with self.lock:
            self.orders[request.order_id] = {
                "items":        list(request.items),
                "order_clock":  self.zero_clocks()
            }
        print("INFO: Suggestions InitOrder response sent.")
        return empty_pb2.Empty()
    
    # event (f) – generate suggestions based on items
    def GetSuggestions(self, request, context):
        print("INFO: Suggestions request received:")

        response = s_pb2.SuggestionsResponse()
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: Suggestions ingress vector clock: {ingress_clock} for order_id: {request.order_id}")            
            order = self.orders.get(request.order_id)   
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: Suggestions updated vector clock: {order['order_clock']} for order_id: {request.order_id}")
            failed = False
        # static book list
        books = [
            {"bookId": "101", "title": "Clean Code",               "author": "Robert C. Martin"},
            {"bookId": "102", "title": "The Pragmatic Programmer", "author": "Andrew Hunt"},
            {"bookId": "103", "title": "Design Patterns",          "author": "GoF"}
        ]

        # Simple logic: return first 2 books
        for b in books[:2]:
            book        = response.books.add()
            book.bookId = b["bookId"]
            book.title  = b["title"]
            book.author = b["author"]
        response.vector_clock.update(order["order_clock"])
        response.failed = failed

        return response
    
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


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    s_pb2_grpc.add_SuggestionsServiceServicer_to_server(
        SuggestionsService(), server
    )

    server.add_insecure_port("[::]:50053")
    server.start()
    print("INFO: Suggestions service running on 50053")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()