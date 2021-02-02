package org.apache.flink.statefun.sdk.java;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;

public final class ValueSpec<T> {

  public static Untyped named(String name) {
    Objects.requireNonNull(name);
    return new Untyped(name);
  }

  private final String name;
  private final Expiration expiration;
  private final Type<T> type;

  private ValueSpec(Untyped untyped, Type<T> type) {
    Objects.requireNonNull(untyped);
    Objects.requireNonNull(type);
    this.name = untyped.stateName;
    this.expiration = untyped.expiration;
    this.type = Objects.requireNonNull(type);
  }

  public String name() {
    return name;
  }

  public Expiration expiration() {
    return expiration;
  }

  public TypeName typeName() {
    return type.typeName();
  }

  public Type<T> type() {
    return type;
  }

  public static final class Untyped {
    private final String stateName;
    private Expiration expiration = Expiration.none();

    public Untyped(String name) {
      this.stateName = Objects.requireNonNull(name);
    }

    public Untyped thatExpireAfterWrite(Duration duration) {
      this.expiration = Expiration.expireAfterWriting(duration);
      return this;
    }

    public Untyped thatExpiresAfterReadOrWrite(Duration duration) {
      this.expiration = Expiration.expireAfterReadingOrWriting(duration);
      return this;
    }

    public ValueSpec<Integer> withIntType() {
      return withCustomType(Types.integerType());
    }

    public ValueSpec<Long> withLongType() {
      return withCustomType(Types.longType());
    }

    public ValueSpec<Float> withFloatType() {
      return withCustomType(Types.floatType());
    }

    public ValueSpec<Double> withDoubleType() {
      return withCustomType(Types.doubleType());
    }

    public ValueSpec<String> withUtf8String() {
      return withCustomType(Types.stringType());
    }

    public ValueSpec<Boolean> withBooleanType() {
      return new ValueSpec<>(this, Types.booleanType());
    }

    public <T> ValueSpec<T> withCustomType(Type<T> type) {
      Objects.requireNonNull(type);
      return new ValueSpec<>(this, type);
    }
  }
}
