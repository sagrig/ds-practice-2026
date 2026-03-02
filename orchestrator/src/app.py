import sys
import os

import threading

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

# fraud detextion gRPC
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

# transaction verification gRPC
tv_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, tv_grpc_path)
import transaction_verification_pb2 as tv_pb2
import transaction_verification_pb2_grpc as tv_pb2_grpc

# suggestions gRPC
s_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, s_grpc_path)
import suggestions_pb2 as s_pb2
import suggestions_pb2_grpc as s_pb2_grpc

import grpc

def verify_transaction(user, items):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = tv_pb2_grpc.TransactionServiceStub(channel)

        request = tv_pb2.TransactionRequest(
            user=user,
            items=items
        )

        response = stub.VerifyTransaction(request)
    return response.valid

def get_suggestions(items):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = s_pb2_grpc.SuggestionsServiceStub(channel)

        request = s_pb2.SuggestionsRequest(
            items=items
        )

        response = stub.GetSuggestions(request)

    books = []
    for b in response.books:
        books.append({
            "bookId": b.bookId,
            "title": b.title,
            "author": b.author
        })

    return books

def check_fraud(card_number, order_amount):
    # Establish a connection with the fraud-detection gRPC service.
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudServiceStub(channel)
        request = fraud_detection.FraudRequest(
            card_number=card_number,
            order_amount=order_amount
        )

        response = stub.CheckFraud(request)
    return response.is_fraud

# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS
import json

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.
@app.route('/', methods=['GET'])
def index():
    return "Orchestrator runs successfully!"

@app.route('/checkout', methods=['POST'])
def checkout():
    print("Orchestrator received /checkout request.")
    
    request_data = request.get_json()

    if not request_data:
        norequest_error_response = {
            "code": "400",
            "message": "Invalid or empty JSON body."            
        }
        return norequest_error_response, 400

    if not request_data.get("items"):
        items_error_response = {
            "code": "400",
            "message": "Order must contain at least one item."
        }
        return items_error_response, 400
    
    if not request_data.get("termsAndConditionsAccepted"):
        terms_error_response = {
            "code": "400",
            "message": "Terms and Conditions must be accepted."
        }
        return terms_error_response, 400

    print("Orchestrator validated items and terms fields.")
    print("Orchestrator spawns worker threads.")

    results = {}
    def fraud_worker():
        card_number = request_data.get("creditCard", {}).get("number", "")
        order_amount = float(len(request_data.get("items", [])) * 100)
        results["fraud"] = check_fraud(card_number, order_amount)

    def transaction_worker():
        user = request_data.get("user", {}).get("name", "")
        items = [item["name"] for item in request_data.get("items", [])]
        results["transaction_valid"] = verify_transaction(user, items)

    def suggestions_worker():
        items = [item["name"] for item in request_data.get("items", [])]
        results["suggestions"] = get_suggestions(items)
    
    fraud_thread       = threading.Thread(target=fraud_worker)
    transaction_thread = threading.Thread(target=transaction_worker)
    suggestions_thread = threading.Thread(target=suggestions_worker)

    fraud_thread.start()
    transaction_thread.start()
    suggestions_thread.start()

    fraud_thread.join()
    transaction_thread.join()
    suggestions_thread.join()

    if results.get("fraud"):
        status = "Order Rejected!"
        suggested_books = []
    elif not results.get("transaction_valid"):
        status = "Order Rejected!"
        suggested_books = []
    elif request_data.get("discountCode") == "INVALID":
        status = "Order Rejected"
        suggested_books = []
    else:        
        status = "Order Approved."
        suggested_books = results.get("suggestions", [])

    order_status_response = {
        'orderId':        '12345',
        'status':         status,
        'suggestedBooks': suggested_books
    }

    return order_status_response, 200


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
