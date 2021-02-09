/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.sdk.java.io;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class KafkaEgressMessage {

  public static Builder forEgress(TypeName targetEgressId) {
    Objects.requireNonNull(targetEgressId);
    return new Builder(targetEgressId);
  }

  public static final class Builder {
    private static final TypeName KAFKA_PRODUCER_RECORD_TYPENAME =
        TypeName.typeNameOf(
            "type.googleapis.com", KafkaProducerRecord.getDescriptor().getFullName());

    private final TypeName targetEgressId;
    private String targetTopic;
    private ByteString keyBytes;
    private ByteString value;

    private Builder(TypeName targetEgressId) {
      this.targetEgressId = targetEgressId;
    }

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
              .setTypenameBytes(ApiExtension.typeNameByteString(KAFKA_PRODUCER_RECORD_TYPENAME))
              .setValue(record.toByteString())
              .build();

      return new EgressMessageWrapper(targetEgressId, typedValue);
    }
  }
}
