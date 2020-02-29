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

from google.protobuf.any_pb2 import Any
import inspect


class SdkAddress(object):
    def __init__(self, namespace, type, identity):
        self.namespace = namespace
        self.type = type
        self.identity = identity

    def __repr__(self):
        return "%s/%s/%s" % (self.namespace, self.type, self.identity)

    def typename(self):
        return "%s/%s" % (self.namespace, self.type)


class AnyStateHandle(object):
    def __init__(self, any_bytes):
        any = Any()
        any.ParseFromString(any_bytes)
        self.any = any
        self.value_bytes = any_bytes
        self.modified = False
        self.deleted = False

    def bytes(self):
        if self.deleted:
            raise AssertionError("can not obtain the bytes of a delete handle")
        if self.modified:
            return self.value.SerializeToString()
        else:
            return self.value_bytes

    @property
    def value(self):
        """returns the current value of this state"""
        return self.any

    @value.setter
    def value(self, any):
        """updates this value to the supplied value, and also marks this state as modified"""
        self.any = any
        self.modified = True
        self.deleted = False

    @value.deleter
    def value(self):
        """marks this state as deleted and also as modified"""
        self.any = None
        self.deleted = True
        self.modified = True


class StatefulFunction(object):

    def __init__(self, fun, expected_messages=None):
        if expected_messages is None:
            self.known_messages = None
        else:
            self.known_messages = expected_messages[:]
        self.func = fun

    def unpack_any(self, any: Any):
        if self.known_messages is None:
            return None
        for known_message in self.known_messages:
            if any.Is(known_message):
                value = known_message()
                any.Unpack(value)
                return value

        raise ValueError("Unknown message type " + any.type_url)

def parse_typename(typename):
    """parses a string of type namespace/type into a tuple of (namespace, type)"""
    if typename is None:
        raise ValueError("function type must be provided")
    idx = typename.rfind("/")
    if idx < 0:
        raise ValueError("function type must be of the from namespace/name")
    namespace = typename[:idx]
    if not namespace:
        raise ValueError("function type's namespace must not be empty")
    type = typename[idx + 1:]
    if not type:
        raise ValueError("function type's name must not be empty")
    return namespace, type


def deduce_protobuf_types(fn):
    spec = inspect.getfullargspec(fn)
    if not spec:
        return None
    if len(spec.args) != 2:
        raise TypeError("A stateful function must have two arguments: a context and a message. but got ", spec.args)
    message_arg_name = spec.args[1]  # has to be the second element
    if message_arg_name not in spec.annotations:
        return None
    message_annotation = spec.annotations[message_arg_name]
    if inspect.isclass(message_annotation):
        return [message_annotation]
    try:
        # it is not a class, then it is only allowed to be
        # typing.SpecialForm('Union')
        return list(message_annotation.__args__)
    except Exception:
        return None



class StatefulFunctions:
    def __init__(self):
        self.functions = {}

    def register(self, typename: str, fun):
        """registers a StatefulFunction function instance, under the given namespace with the given function type. """
        if fun is None:
            raise ValueError("function instance must be provided")
        namespace, type = parse_typename(typename)
        expected_messages = deduce_protobuf_types(fun)
        self.functions[(namespace, type)] = StatefulFunction(fun, expected_messages)

    def bind(self, typename):
        """wraps a StatefulFunction instance with a given namespace and type.
           for example:
            s = StateFun()

            @s.define("com.foo.bar/greeter")
            def greeter(context, message):
                print("Hi there")

            This would add an invokable stateful function that can accept messages
            sent to "com.foo.bar/greeter".
         """

        def wrapper(function):
            self.register(typename, function)
            return function

        return wrapper

    def for_type(self, namespace, type):
        return self.functions[(namespace, type)]
