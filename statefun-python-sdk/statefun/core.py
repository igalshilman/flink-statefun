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


class StatefulFunctions:
    def __init__(self):
        self.functions = {}

    def register(self, typename: str, fun):
        """registers a StatefulFunction function instance, under the given namespace with the given function type. """
        if fun is None:
            raise ValueError("function instance must be provided")
        namespace, type = parse_typename(typename)
        self.functions[(namespace, type)] = fun

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
