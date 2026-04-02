# -*- coding: utf-8 -*-
# Manual protobuf module for order_executor.proto.

from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

_sym_db = _symbol_database.Default()

_file_proto = _descriptor_pb2.FileDescriptorProto()
_file_proto.name = "order_executor.proto"
_file_proto.package = "order_executor"
_file_proto.syntax = "proto3"

_empty = _file_proto.message_type.add()
_empty.name = "Empty"

_vote_request = _file_proto.message_type.add()
_vote_request.name = "VoteRequest"
_field = _vote_request.field.add()
_field.name = "term"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_field = _vote_request.field.add()
_field.name = "candidate_id"
_field.number = 2
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING

_vote_response = _file_proto.message_type.add()
_vote_response.name = "VoteResponse"
_field = _vote_response.field.add()
_field.name = "term"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_field = _vote_response.field.add()
_field.name = "vote_granted"
_field.number = 2
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
_field = _vote_response.field.add()
_field.name = "responder_id"
_field.number = 3
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING

_append_entries_request = _file_proto.message_type.add()
_append_entries_request.name = "AppendEntriesRequest"
_field = _append_entries_request.field.add()
_field.name = "term"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_field = _append_entries_request.field.add()
_field.name = "leader_id"
_field.number = 2
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING

_append_entries_response = _file_proto.message_type.add()
_append_entries_response.name = "AppendEntriesResponse"
_field = _append_entries_response.field.add()
_field.name = "term"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_field = _append_entries_response.field.add()
_field.name = "success"
_field.number = 2
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
_field = _append_entries_response.field.add()
_field.name = "responder_id"
_field.number = 3
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING

_status_request = _file_proto.message_type.add()
_status_request.name = "StatusRequest"
_field = _status_request.field.add()
_field.name = "empty"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
_field.type_name = ".order_executor.Empty"

_status_response = _file_proto.message_type.add()
_status_response.name = "StatusResponse"
_field = _status_response.field.add()
_field.name = "node_id"
_field.number = 1
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING
_field = _status_response.field.add()
_field.name = "role"
_field.number = 2
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING
_field = _status_response.field.add()
_field.name = "current_term"
_field.number = 3
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_INT32
_field = _status_response.field.add()
_field.name = "leader_id"
_field.number = 4
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING
_field = _status_response.field.add()
_field.name = "peers"
_field.number = 5
_field.label = _descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
_field.type = _descriptor_pb2.FieldDescriptorProto.TYPE_STRING

_service = _file_proto.service.add()
_service.name = "OrderExecutorService"
_method = _service.method.add()
_method.name = "RequestVote"
_method.input_type = ".order_executor.VoteRequest"
_method.output_type = ".order_executor.VoteResponse"
_method = _service.method.add()
_method.name = "AppendEntries"
_method.input_type = ".order_executor.AppendEntriesRequest"
_method.output_type = ".order_executor.AppendEntriesResponse"
_method = _service.method.add()
_method.name = "GetStatus"
_method.input_type = ".order_executor.StatusRequest"
_method.output_type = ".order_executor.StatusResponse"

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(_file_proto.SerializeToString())
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "order_executor_pb2", _globals)

if False:
    DESCRIPTOR._options = None
