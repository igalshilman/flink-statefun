package org.apache.flink.statefun.sdk.java.example;

import static org.apache.flink.statefun.sdk.java.TypeName.typeNameFromString;

import com.google.protobuf.Timestamp;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.io.KafkaRecord;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public class Gordon implements StatefulFunction {

  private static final TypeName KAFKA_EGRESS = typeNameFromString("com.mycomp.foo/bar");
  private static final ValueSpec<String> USER_NAME_VALUE =
      ValueSpec.named("user_name").withUtf8String();

  // imagine FromFunction is some user defined type

  private static final Type<Timestamp> USER_DEFINED_PROTOBUF_TYPE =
      SimpleType.simpleTypeFrom(
          typeNameFromString("com.igal/" + Timestamp.getDescriptor().getFullName()),
          Timestamp::toByteArray,
          Timestamp::parseFrom);

  private static final ValueSpec<Timestamp> USER_DEFINED_PROTOBUF_VALUE =
      ValueSpec.named("user_json").withCustomType(USER_DEFINED_PROTOBUF_TYPE);

  @Override
  public CompletableFuture<?> apply(Context context, Message argument) throws Throwable {
    // demo string message
    if (argument.isUtf8String()) {
      System.out.println(argument.asUtf8String());
      return CompletableFuture.completedFuture(null);
    }

    // demo sending a Kafka egress
    context.send(
        KafkaRecord.forEgress(KAFKA_EGRESS).withUtf8Key("foo").withUtf8Value("bar").build());

    // demo state
    AddressScopedStorage storage = context.storage();

    // demo string
    String userName = storage.get(USER_NAME_VALUE).orElse("");
    storage.set(USER_NAME_VALUE, userName + "!");

    // demo complex state
    Optional<Timestamp> userProtobuf = storage.get(USER_DEFINED_PROTOBUF_VALUE);
    System.out.println(userProtobuf);

    // demo complex message parsing
    if (argument.is(USER_DEFINED_PROTOBUF_TYPE)) {
      Timestamp userValue = argument.as(USER_DEFINED_PROTOBUF_TYPE);
      System.out.println(userValue);
    }

    return CompletableFuture.runAsync(
        () -> {
          // demo async state removal.
          storage.remove(USER_DEFINED_PROTOBUF_VALUE);
        });
  }
}
