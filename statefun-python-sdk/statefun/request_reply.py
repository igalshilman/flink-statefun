################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from statefun.core import SdkAddress
from statefun.core import StatefulFunction
from statefun.core import AnyStateHandle
from statefun.core import parse_typename

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction

from google.protobuf.any_pb2 import Any


class RequestReplyHandler:
    def __init__(self, functions):
        self.functions = functions

    def __call__(self, request_bytes):
        request = ToFunction()
        request.ParseFromString(request_bytes)
        reply = self.handle_invocation(request)
        return reply.SerializeToString()

    def handle_invocation(self, to_function):
        #
        # setup
        #
        context = BatchContext(to_function.invocation.target, to_function.invocation.state)
        target_function = self.functions.for_type(context.address.namespace, context.address.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_function)
        #
        # process the batch
        #
        batch = to_function.invocation.invocations
        self.invoke_batch(batch, context, target_function)
        #
        # prepare an invocation result that represents the batch
        #
        from_function = FromFunction()
        invocation_result = from_function.invocation_result
        self.add_mutations(context, invocation_result)
        self.add_outgoing_messages(context, invocation_result)
        return from_function

    @staticmethod
    def add_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages
        for typename, id, message in context.messages:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id

            if isinstance(message, Any):
                outgoing.argument.CopyFrom(message)
            else:
                outgoing.argument.Pack(message)

    @staticmethod
    def add_mutations(context, invocation_result):
        for name, handle in context.states.items():
            if not handle.modified:
                continue
            mutation = invocation_result.state_mutations.add()

            mutation.state_name = name
            if handle.deleted:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('DELETE')
            else:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('MODIFY')
                mutation.state_value = handle.bytes()

    @staticmethod
    def invoke_batch(batch, context, target_function: StatefulFunction):
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                fun(context, invocation.argument)
            else:
                fun(context, unpacked)


class BatchContext(object):
    def __init__(self, target, states):
        # populate the state store with the eager state values provided in the batch
        self.states = {s.state_name: AnyStateHandle(s.state_value) for s in states}
        # remember own address
        self.address = SdkAddress(target.namespace, target.type, target.id)
        # the caller address would be set for each individual invocation in the batch
        self.caller = None
        # outgoing messages
        self.messages = []

    def prepare(self, invocation):
        """setup per invocation """
        if invocation.caller:
            caller = invocation.caller
            self.caller = SdkAddress(caller.namespace, caller.type, caller.id)
        else:
            self.caller = None

    # --------------------------------------------------------------------------------------
    # state access
    # --------------------------------------------------------------------------------------

    def state(self, name):
        if name not in self.states:
            raise KeyError('unknown state name ' + name + ', states needed to be explicitly registered in module.yaml')
        return self.states[name]

    def __getitem__(self, name):
        return self.state(name).value

    def __delitem__(self, name):
        state = self.state(name)
        del state.value

    def __setitem__(self, name, value):
        state = self.state(name)
        state.value = value

    # --------------------------------------------------------------------------------------
    # messages
    # --------------------------------------------------------------------------------------

    def send(self, typename, id, message):
        out = (typename, id, message)
        self.messages.append(out)

    def reply(self, message):
        caller = self.caller
        if not caller:
            raise AssertionError(
                "Unable to reply without a caller. Was this message was sent directly from an ingress?")
        self.send(caller.typename(), caller.identity, message)
