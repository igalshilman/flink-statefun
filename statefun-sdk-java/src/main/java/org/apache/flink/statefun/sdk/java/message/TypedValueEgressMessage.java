package org.apache.flink.statefun.sdk.java.message;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class TypedValueEgressMessage implements EgressMessage {
  private final TypedValue typedValue;

  public TypedValueEgressMessage(TypedValue typedValue) {
    this.typedValue = typedValue;
  }

  public TypedValue typedValue() {
    return typedValue;
  }
}
