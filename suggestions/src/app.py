import sys
import os
import grpc
import threading
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, grpc_path)

import suggestions_pb2 as s_pb2
import suggestions_pb2_grpc as s_pb2_grpc

NODES     = ["orchestrator", "transaction", "fraud", "suggestions", "order_queue"]
THIS_NODE = "suggestions"

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

# -- Suggestions Service API --
class SuggestionsService(s_pb2_grpc.SuggestionsServiceServicer):

    def InitOrder(self, request, context):
        print("INFO: Suggestions InitOrder request received:")
        print(f"Order ID: {request.order_id}")
        print(f"Items:    {list(request.items)}")

        clock_input = dict(request.vector_clock)

        with LOCK:
            local_clock = merge_clock(zero_clocks(), clock_input)
            local_clock = tick(local_clock, THIS_NODE)

            ORDERS[request.order_id] = {
                "items":        list(request.items),
                "vector_clock": local_clock
            }

        response = s_pb2.InitOrderResponse(
            ok      = True,
            message = "Suggestions service initialized order."
        )
        response.vector_clock.update(local_clock)

        print("INFO: Suggestions InitOrder response sent.")
        return response
    
    def GetSuggestions(self, request, context):
        response = s_pb2.SuggestionsResponse()

        print("INFO: Suggestions request received:")
        print(f"Order ID: {request.order_id}")

        with LOCK:
            if request.order_id not in ORDERS:
                print("ERROR: Unknown order_id in suggestions service.")
                response.vector_clock.update(zero_clocks())
                return response

            order_state = ORDERS[request.order_id]

            local_clock = merge_clock(order_state["vector_clock"], 
                                      dict(request.vector_clock))
            local_clock = tick(local_clock, THIS_NODE)
            order_state["vector_clock"] = local_clock

            items = order_state["items"]  
        print(f"INFO: Cached items: {items}")

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

        response.vector_clock.update(local_clock)

        print("INFO: Suggestions response:")
        print(f"Suggestions book number: {len(response.books)}")
        return response


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