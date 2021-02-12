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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

/**
 * Utility for pairing registered {@link ValueSpec}s with values provided by the protocol's {@link
 * ToFunction} message.
 */
public final class StateValueContexts {

  public static final class StateValueContext<T> {
    private final ValueSpec<T> spec;
    private final ToFunction.PersistedValue protocolValue;

    StateValueContext(ValueSpec<T> spec, ToFunction.PersistedValue protocolValue) {
      this.spec = Objects.requireNonNull(spec);
      this.protocolValue = Objects.requireNonNull(protocolValue);
    }

    public ValueSpec<T> spec() {
      return spec;
    }

    public ToFunction.PersistedValue protocolValue() {
      return protocolValue;
    }
  }

  public static final class ResolutionResult {
    private final Map<String, StateValueContext<?>> resolved;
    private final Set<ValueSpec<?>> missingValues;

    ResolutionResult(Map<String, StateValueContext<?>> resolved, Set<ValueSpec<?>> missingValues) {
      this.resolved = resolved;
      this.missingValues = missingValues;
    }

    public boolean hasMissingValues() {
      return missingValues != null;
    }

    public Map<String, StateValueContext<?>> resolved() {
      return resolved;
    }

    public Set<ValueSpec<?>> missingValues() {
      return missingValues;
    }
  }

  public static ResolutionResult resolve(
      Map<String, ValueSpec<?>> registeredSpecs,
      List<ToFunction.PersistedValue> protocolProvidedValues) {
    final Map<String, ToFunction.PersistedValue> providedValuesIndex =
        createStateValuesIndex(protocolProvidedValues);

    final Set<ValueSpec<?>> statesWithMissingValue =
        statesWithMissingValue(registeredSpecs, providedValuesIndex);
    if (!statesWithMissingValue.isEmpty()) {
      return new ResolutionResult(null, statesWithMissingValue);
    }

    final Map<String, StateValueContext<?>> resolvedStateValues =
        new HashMap<>(registeredSpecs.size());
    registeredSpecs.forEach(
        (stateName, spec) -> {
          final ToFunction.PersistedValue providedValue = providedValuesIndex.get(stateName);
          resolvedStateValues.put(stateName, new StateValueContext<>(spec, providedValue));
        });
    return new ResolutionResult(resolvedStateValues, null);
  }

  private static Map<String, ToFunction.PersistedValue> createStateValuesIndex(
      List<ToFunction.PersistedValue> stateValues) {
    final Map<String, ToFunction.PersistedValue> index = new HashMap<>(stateValues.size());
    stateValues.forEach(value -> index.put(value.getStateName(), value));
    return index;
  }

  private static Set<ValueSpec<?>> statesWithMissingValue(
      Map<String, ValueSpec<?>> registeredSpecs,
      Map<String, ToFunction.PersistedValue> providedValuesIndex) {
    final Set<ValueSpec<?>> statesWithMissingValue = new HashSet<>();
    registeredSpecs.forEach(
        (stateName, spec) -> {
          if (!providedValuesIndex.containsKey(stateName)) {
            statesWithMissingValue.add(spec);
          }
        });
    return statesWithMissingValue;
  }
}
