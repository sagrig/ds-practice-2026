import sys
import os
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, grpc_path)

import suggestions_pb2 as s_pb2
import suggestions_pb2_grpc as s_pb2_grpc


class SuggestionsService(s_pb2_grpc.SuggestionsServiceServicer):

    def GetSuggestions(self, request, context):
        response = s_pb2.SuggestionsResponse()

        print("Suggestions request received:")
        print("Items: ", request.items)

        # Static book list
        books = [
            {"bookId": "101", "title": "Clean Code", "author": "Robert C. Martin"},
            {"bookId": "102", "title": "The Pragmatic Programmer", "author": "Andrew Hunt"},
            {"bookId": "103", "title": "Design Patterns", "author": "GoF"}
        ]

        # Simple logic: return first 2 books
        for b in books[:2]:
            book = response.books.add()
            book.bookId = b["bookId"]
            book.title = b["title"]
            book.author = b["author"]
        
        print("Suggestions response:")
        print(f"Suggestions book number: {len(response.books)}")
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    s_pb2_grpc.add_SuggestionsServiceServicer_to_server(
        SuggestionsService(), server
    )

    server.add_insecure_port("[::]:50053")
    server.start()
    print("Suggestions service running on 50053")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()