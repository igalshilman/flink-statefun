package org.apache.flink.statefun.sdk.java.io;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.TypedValueEgressMessage;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class KafkaRecord {

  public static Builder forEgress(TypeName greetingsEgress) {
    return new Builder();
  }

  public static final class Builder {
    private String targetTopic;
    private ByteString keyBytes;
    private ByteString value;

    public Builder withTopic(String topic) {
      this.targetTopic = Objects.requireNonNull(topic);
      return this;
    }

    public Builder withKey(byte[] key) {
      Objects.requireNonNull(key);
      this.keyBytes = ByteString.copyFrom(key);
      return this;
    }

    public Builder withUtf8Key(String key) {
      Objects.requireNonNull(key);
      this.keyBytes = ByteString.copyFromUtf8(key);
      return this;
    }

    public Builder withUtf8Value(String value) {
      Objects.requireNonNull(value);
      this.value = ByteString.copyFromUtf8(value);
      return this;
    }

    public Builder withValue(byte[] value) {
      Objects.requireNonNull(value);
      this.value = ByteString.copyFrom(value);
      return this;
    }

    public EgressMessage build() {
      if (targetTopic == null) {
        throw new IllegalStateException("A Kafka record requires a target topic.");
      }
      if (value == null) {
        throw new IllegalStateException("A Kafka record requires value bytes");
      }
      KafkaProducerRecord.Builder builder =
          KafkaProducerRecord.newBuilder().setTopic(targetTopic).setValueBytes(value);
      if (keyBytes != null) {
        builder.setKeyBytes(keyBytes);
      }
      KafkaProducerRecord record = builder.build();
      TypedValue typedValue =
          TypedValue.newBuilder()
              .setTypename("io.statefun.types/" + record.getDescriptorForType().getFullName())
              .setValue(record.toByteString())
              .build();

      return new TypedValueEgressMessage(typedValue);
    }
  }
}
