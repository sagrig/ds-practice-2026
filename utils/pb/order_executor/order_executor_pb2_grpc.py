import grpc

import order_executor_pb2 as order__executor__pb2


class OrderExecutorServiceStub(object):
    def __init__(self, channel):
        self.RequestVote = channel.unary_unary(
            "/order_executor.OrderExecutorService/RequestVote",
            request_serializer=order__executor__pb2.VoteRequest.SerializeToString,
            response_deserializer=order__executor__pb2.VoteResponse.FromString,
        )
        self.AppendEntries = channel.unary_unary(
            "/order_executor.OrderExecutorService/AppendEntries",
            request_serializer=order__executor__pb2.AppendEntriesRequest.SerializeToString,
            response_deserializer=order__executor__pb2.AppendEntriesResponse.FromString,
        )
        self.GetStatus = channel.unary_unary(
            "/order_executor.OrderExecutorService/GetStatus",
            request_serializer=order__executor__pb2.StatusRequest.SerializeToString,
            response_deserializer=order__executor__pb2.StatusResponse.FromString,
        )


class OrderExecutorServiceServicer(object):
    def RequestVote(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def AppendEntries(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetStatus(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_OrderExecutorServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "RequestVote": grpc.unary_unary_rpc_method_handler(
            servicer.RequestVote,
            request_deserializer=order__executor__pb2.VoteRequest.FromString,
            response_serializer=order__executor__pb2.VoteResponse.SerializeToString,
        ),
        "AppendEntries": grpc.unary_unary_rpc_method_handler(
            servicer.AppendEntries,
            request_deserializer=order__executor__pb2.AppendEntriesRequest.FromString,
            response_serializer=order__executor__pb2.AppendEntriesResponse.SerializeToString,
        ),
        "GetStatus": grpc.unary_unary_rpc_method_handler(
            servicer.GetStatus,
            request_deserializer=order__executor__pb2.StatusRequest.FromString,
            response_serializer=order__executor__pb2.StatusResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "order_executor.OrderExecutorService",
        rpc_method_handlers,
    )
    server.add_generic_rpc_handlers((generic_handler,))
