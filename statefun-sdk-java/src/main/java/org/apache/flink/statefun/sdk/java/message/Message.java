package org.apache.flink.statefun.sdk.java.message;

import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.types.Type;

public interface Message {
  Address targetAddress();

  boolean isLong();

  long asLong();

  boolean isUtf8String();

  String asUtf8String();

  boolean isInt();

  int asInt();

  boolean isBoolean();

  boolean asBoolean();

  boolean isFloat();

  float asFloat();

  boolean isDouble();

  double asDouble();

  <T> boolean is(Type<T> type);

  <T> T as(Type<T> type);
}
