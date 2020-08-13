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
package org.apache.flink.statefun.flink.core.common;

import java.util.Objects;
import org.apache.flink.statefun.flink.core.feedback.PriorityAwareExecutor;
import org.apache.flink.streaming.api.operators.MailboxExecutor;

public final class PriorityAwareMailboxExecutorFacade implements PriorityAwareExecutor {
  private final MailboxExecutor operatorPriorityExecutor;
  private final MailboxExecutor highestPriorityExecutor;
  private final String name;

  public PriorityAwareMailboxExecutorFacade(
      MailboxExecutor operatorPriorityExecutor,
      MailboxExecutor highestPriorityExecutor,
      String name) {
    this.operatorPriorityExecutor = Objects.requireNonNull(operatorPriorityExecutor);
    this.highestPriorityExecutor = Objects.requireNonNull(highestPriorityExecutor);
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public void executeWithHighestPriority(Runnable command) {
    highestPriorityExecutor.execute(command::run, name);
  }

  @Override
  public void executeWithOperatorsPriority(Runnable command) {
    operatorPriorityExecutor.execute(command::run, name);
  }
}
