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

import unittest

from google.protobuf.any_pb2 import Any

from tests.examples_pb2 import LoginEvent, SeenCount
from statefun.request_reply_pb2 import ToFunction, FromFunction
from statefun import RequestReplyHandler
from statefun.core import StatefulFunctions, StatefulFunction


class InvocationBuilder(object):
    """builder for the ToFunction message"""

    def __init__(self):
        self.to_function = ToFunction()

    def with_target(self, ns, type, id):
        InvocationBuilder.set_address(ns, type, id, self.to_function.invocation.target)
        return self

    def with_state(self, name, value=None):
        state = self.to_function.invocation.state.add()
        state.state_name = name
        if value:
            any = Any()
            any.Pack(value)
            state.state_value = any.SerializeToString()
        return self

    def with_invocation(self, arg, caller=None):
        invocation = self.to_function.invocation.invocations.add()
        if caller:
            (ns, type, id) = caller
            InvocationBuilder.set_address(ns, type, id, invocation.caller)
        invocation.argument.Pack(arg)
        return self

    def SerializeToString(self):
        return self.to_function.SerializeToString()

    @staticmethod
    def set_address(namespace, type, id, address):
        address.namespace = namespace
        address.type = type
        address.id = id


def register_and_invoke(typename, fn, to: ToFunction) -> FromFunction:
    functions = StatefulFunctions()
    functions.register(typename, fn)
    handler = RequestReplyHandler(functions)
    f = FromFunction()
    f.ParseFromString(handler(to.SerializeToString()))
    return f


class RequestReplyTestCase(unittest.TestCase):

    def test_integration(self):
        def fun(context, message):
            seen = context.state('seen').unpack(SeenCount)
            context.pack_and_reply(seen)

        # build the invocation
        builder = InvocationBuilder()
        builder.with_target("org.foo", "greeter", "0")

        seen = SeenCount()
        seen.seen = 100
        builder.with_state("seen")

        arg = LoginEvent()
        arg.user_name = "user-1"
        builder.with_invocation(arg, ("org.foo", "greeter-java", "0"))

        # invoke
        from_function = register_and_invoke("org.foo/greeter", fun, builder.to_function)

        # assert the result
        # print(from_function)
        self.assertIsNotNone(from_function)
