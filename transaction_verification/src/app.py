import sys
import os
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, grpc_path)

import transaction_verification_pb2 as tv_pb2
import transaction_verification_pb2_grpc as tv_pb2_grpc


class TransactionService(tv_pb2_grpc.TransactionServiceServicer):

    def VerifyTransaction(self, request, context):
        print("Transaction verification request received:")
        print(f"User:  {request.user}")
        print(f"Items: {request.items}")

        response = tv_pb2.TransactionResponse()

        if request.items:
            response.valid = True
        else:
            response.valid = False

        print("Transaction verification response:")
        print(f"Valid: {response.valid}")

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    tv_pb2_grpc.add_TransactionServiceServicer_to_server(
        TransactionService(), server
    )

    server.add_insecure_port("[::]:50052")
    server.start()
    print("Transaction Verification running on 50052")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()