package org.apache.flink.statefun.sdk.java.types;

import org.apache.flink.statefun.sdk.java.TypeName;

public interface Type<T> {

  TypeName typeName();

  TypeSerializer<T> typeSerializer();
}
