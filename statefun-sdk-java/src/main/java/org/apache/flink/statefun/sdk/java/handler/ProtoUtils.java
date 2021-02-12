package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec.ExpireMode;
import static org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.ExpirationSpec.newBuilder;

import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.Expiration;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueSpec;

class ProtoUtils {
  private ProtoUtils() {}

  static Address protoAddressFromSdk(org.apache.flink.statefun.sdk.java.Address address) {
    return Address.newBuilder()
        .setNamespace(address.type().namespace())
        .setType(address.type().name())
        .setId(address.id())
        .build();
  }

  static org.apache.flink.statefun.sdk.java.Address sdkAddressFromProto(Address address) {
    if (address == null
        || (address.getNamespace().isEmpty()
            && address.getType().isEmpty()
            && address.getId().isEmpty())) {
      return null;
    }
    return new org.apache.flink.statefun.sdk.java.Address(
        TypeName.typeNameOf(address.getNamespace(), address.getType()), address.getId());
  }

  static PersistedValueSpec.Builder protoFromValueSpec(ValueSpec<?> valueSpec) {
    PersistedValueSpec.Builder specBuilder =
        PersistedValueSpec.newBuilder()
            .setStateNameBytes(ApiExtension.stateNameByteString(valueSpec))
            .setTypeTypenameBytes(ApiExtension.typeNameByteString(valueSpec.typeName()));

    if (valueSpec.expiration().mode() == Expiration.Mode.NONE) {
      return specBuilder;
    }

    ExpireMode mode =
        valueSpec.expiration().mode() == Expiration.Mode.AFTER_READ_OR_WRITE
            ? ExpireMode.AFTER_INVOKE
            : ExpireMode.AFTER_WRITE;
    long value = valueSpec.expiration().duration().toMillis();

    specBuilder.setExpirationSpec(newBuilder().setExpireAfterMillis(value).setMode(mode));
    return specBuilder;
  }
}
