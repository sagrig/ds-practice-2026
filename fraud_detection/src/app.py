import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures

class FraudService(fraud_detection_grpc.FraudServiceServicer):
    # Create an RPC function to say hello
    def CheckFraud(self, request, context):
        response = fraud_detection.FraudResponse()
        if request.card_number.startswith("0000") or request.order_amount > 1000:
            response.is_fraud = True
        else:
            response.is_fraud = False

        print(
            f"Fraud check | Card: {request.card_number} | "
            f"Amount: {request.order_amount} | Fraud: {response.is_fraud}"
        )
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    fraud_detection_grpc.add_FraudServiceServicer_to_server(FraudService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()