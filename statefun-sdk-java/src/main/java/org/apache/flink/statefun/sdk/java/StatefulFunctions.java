package org.apache.flink.statefun.sdk.java;

import java.util.List;
import java.util.function.Supplier;

public class StatefulFunctions {
  public void register(
      TypeName type,
      List<ValueSpec<Long>> valueSpecs,
      Supplier<? extends StatefulFunction> instanceSupplier) {}
}
