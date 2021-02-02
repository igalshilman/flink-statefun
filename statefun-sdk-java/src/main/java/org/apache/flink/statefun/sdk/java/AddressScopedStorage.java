package org.apache.flink.statefun.sdk.java;

import java.util.Optional;

public interface AddressScopedStorage {
  <T> Optional<T> get(ValueSpec<T> descriptor);

  <T> void set(ValueSpec<T> key, T value);

  <T> void remove(ValueSpec<T> key);
}
