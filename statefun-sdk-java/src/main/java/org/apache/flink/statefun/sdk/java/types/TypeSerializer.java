package org.apache.flink.statefun.sdk.java.types;

public interface TypeSerializer<T> {

  byte[] serialize(T value);

  T deserialize(byte[] bytes);
}
