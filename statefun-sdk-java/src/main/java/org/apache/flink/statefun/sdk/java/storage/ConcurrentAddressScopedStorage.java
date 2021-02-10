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

package org.apache.flink.statefun.sdk.java.storage;

import static org.apache.flink.statefun.sdk.java.storage.StateValueContexts.StateValueContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.TypeCharacteristics;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class ConcurrentAddressScopedStorage implements AddressScopedStorage {

  private final Map<String, Cell<?>> cells;

  public ConcurrentAddressScopedStorage(Map<String, StateValueContext<?>> stateValues) {
    this.cells = createCells(stateValues);
  }

  @Override
  public <T> Optional<T> get(ValueSpec<T> valueSpec) {
    final Cell<T> cell = getCellOrThrow(valueSpec.name());
    checkType(cell, valueSpec);
    return cell.get();
  }

  @Override
  public <T> void set(ValueSpec<T> valueSpec, T value) {
    final Cell<T> cell = getCellOrThrow(valueSpec.name());
    checkType(cell, valueSpec);
    cell.set(value);
  }

  @Override
  public <T> void remove(ValueSpec<T> valueSpec) {
    final Cell<T> cell = getCellOrThrow(valueSpec.name());
    checkType(cell, valueSpec);
    cell.remove();
  }

  @SuppressWarnings("unchecked")
  private <T> Cell<T> getCellOrThrow(String stateName) {
    final Cell<T> cell = (Cell<T>) cells.get(stateName);
    if (cell == null) {
      throw new IllegalStorageAccessException(
          stateName, "State does not exist; make sure that this state was registered.");
    }
    return cell;
  }

  private void checkType(Cell<?> cell, ValueSpec<?> descriptor) {
    if (!cell.type().equals(descriptor.typeName())) {
      throw new IllegalStorageAccessException(
          descriptor.name(),
          "Accessed state with incorrect type; state type was registered as "
              + cell.type()
              + ", but was accessed as type "
              + descriptor.typeName());
    }
  }

  // ===============================================================================
  //  Thread-safe state value cells
  // ===============================================================================

  private interface Cell<T> {
    Optional<T> get();

    void set(T value);

    void remove();

    TypeName type();

    Optional<FromFunction.PersistedValueMutation> toProtocolValueMutation();

    static <T> Cell<T> forImmutableType(
        String stateName, TypeName type, Slice valueSlice, TypeSerializer<T> serializer) {
      return new ImmutableTypeCell<>(stateName, type, valueSlice, serializer);
    }

    static <T> Cell<T> forMutableType(
        String stateName, TypeName type, Slice valueSlice, TypeSerializer<T> serializer) {
      return new MutableTypeCell<>(stateName, type, valueSlice, serializer);
    }
  }

  private static final class ImmutableTypeCell<T> implements Cell<T> {
    private final ReentrantLock lock = new ReentrantLock();

    private final TypeSerializer<T> serializer;
    private final String stateName;
    private final TypeName type;
    private final Slice initialValueSlice;

    private CellStatus status = CellStatus.UNMODIFIED;
    private T cachedObject;

    private ImmutableTypeCell(
        String stateName, TypeName type, Slice valueSlice, TypeSerializer<T> serializer) {
      this.stateName = Objects.requireNonNull(stateName);
      this.type = Objects.requireNonNull(type);
      this.initialValueSlice = Objects.requireNonNull(valueSlice);
      this.serializer = Objects.requireNonNull(serializer);
    }

    @Override
    public TypeName type() {
      return type;
    }

    @Override
    public Optional<T> get() {
      lock.lock();
      try {
        if (status == CellStatus.DELETED) {
          return Optional.empty();
        }
        if (cachedObject != null) {
          return Optional.of(cachedObject);
        }
        if (initialValueSlice.readableBytes() > 0) {
          cachedObject = serializer.deserialize(initialValueSlice);
          return Optional.of(cachedObject);
        }
        return Optional.empty();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void set(T value) {
      if (value == null) {
        throw new IllegalStorageAccessException(
            stateName, "Can not set state to NULL. Please use remove() instead.");
      }

      lock.lock();
      try {
        cachedObject = value;
        status = CellStatus.MODIFIED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void remove() {
      lock.lock();
      try {
        cachedObject = null;
        status = CellStatus.DELETED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Optional<PersistedValueMutation> toProtocolValueMutation() {
      switch (status) {
        case MODIFIED:
          final TypedValue.Builder newValue =
              TypedValue.newBuilder()
                  .setTypenameBytes(ApiExtension.typeNameByteString(type))
                  .setValue(SliceProtobufUtil.asByteString(serializer.serialize(cachedObject)));

          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(stateName)
                  .setMutationType(PersistedValueMutation.MutationType.MODIFY)
                  .setStateValue(newValue)
                  .build());
        case DELETED:
          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(stateName)
                  .setMutationType(PersistedValueMutation.MutationType.DELETE)
                  .build());
        case UNMODIFIED:
          return Optional.empty();
        default:
          throw new IllegalStateException("Unknown cell status: " + status);
      }
    }
  }

  private static final class MutableTypeCell<T> implements Cell<T> {
    private final ReentrantLock lock = new ReentrantLock();

    private final TypeSerializer<T> serializer;
    private final String stateName;
    private final TypeName type;

    private Slice valueSlice;
    private CellStatus status = CellStatus.UNMODIFIED;

    private MutableTypeCell(
        String stateName, TypeName type, Slice valueSlice, TypeSerializer<T> serializer) {
      this.stateName = Objects.requireNonNull(stateName);
      this.type = Objects.requireNonNull(type);
      this.valueSlice = Objects.requireNonNull(valueSlice);
      this.serializer = Objects.requireNonNull(serializer);
    }

    @Override
    public TypeName type() {
      return type;
    }

    @Override
    public Optional<T> get() {
      lock.lock();
      try {
        if (valueSlice != null) {
          final T valueObject = serializer.deserialize(valueSlice);
          return Optional.of(valueObject);
        }
        return Optional.empty();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void set(T value) {
      if (value == null) {
        throw new IllegalStorageAccessException(
            stateName, "Can not set state to NULL. Please use remove() instead.");
      }

      lock.lock();
      try {
        valueSlice = serializer.serialize(value);
        status = CellStatus.MODIFIED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void remove() {
      lock.lock();
      try {
        valueSlice = null;
        status = CellStatus.DELETED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Optional<PersistedValueMutation> toProtocolValueMutation() {
      switch (status) {
        case MODIFIED:
          final TypedValue.Builder newValue =
              TypedValue.newBuilder()
                  .setTypenameBytes(ApiExtension.typeNameByteString(type))
                  .setValue(SliceProtobufUtil.asByteString(valueSlice));

          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(stateName)
                  .setMutationType(PersistedValueMutation.MutationType.MODIFY)
                  .setStateValue(newValue)
                  .build());
        case DELETED:
          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(stateName)
                  .setMutationType(PersistedValueMutation.MutationType.DELETE)
                  .build());
        case UNMODIFIED:
          return Optional.empty();
        default:
          throw new IllegalStateException("Unknown cell status: " + status);
      }
    }
  }

  private enum CellStatus {
    UNMODIFIED,
    MODIFIED,
    DELETED
  }

  private static Map<String, Cell<?>> createCells(Map<String, StateValueContext<?>> stateValues) {
    final Map<String, Cell<?>> cells = new HashMap<>(stateValues.size());

    stateValues.forEach(
        (stateName, stateValueContext) -> {
          final TypedValue typedValue = stateValueContext.protocolValue().getStateValue();
          final Slice valueSlice = Slices.wrap(typedValue.getValue().toByteArray());
          final Type<?> stateType = stateValueContext.spec().type();
          final Cell<?> cell =
              stateType.typeCharacteristics().contains(TypeCharacteristics.IMMUTABLE_VALUES)
                  ? Cell.forImmutableType(
                      stateName, stateType.typeName(), valueSlice, stateType.typeSerializer())
                  : Cell.forMutableType(
                      stateName, stateType.typeName(), valueSlice, stateType.typeSerializer());
          cells.put(stateName, cell);
        });

    return cells;
  }
}
