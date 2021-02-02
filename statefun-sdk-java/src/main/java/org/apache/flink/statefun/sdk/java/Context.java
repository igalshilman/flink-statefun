package org.apache.flink.statefun.sdk.java;

import java.time.Duration;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

public interface Context {

  Address self();

  Optional<Address> caller();

  void send(MessageBuilder message);

  void send(EgressMessage message);

  void sendAfter(Duration duration, MessageBuilder message);

  void reply(MessageBuilder message);

  AddressScopedStorage storage();
}
