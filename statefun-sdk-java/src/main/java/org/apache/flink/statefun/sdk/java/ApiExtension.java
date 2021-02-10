package org.apache.flink.statefun.sdk.java;

import com.google.protobuf.ByteString;

public final class ApiExtension {

  public static ByteString typeNameByteString(TypeName typeName) {
    return typeName.typeNameByteString();
  }

  public static ByteString stateNameByteString(ValueSpec<?> spec) {
    return spec.nameByteString();
  }
}
