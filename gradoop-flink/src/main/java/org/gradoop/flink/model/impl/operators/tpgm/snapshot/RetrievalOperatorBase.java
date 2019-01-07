/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.tpgm.snapshot;

import java.util.Objects;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempElementTuple;

/**
 * Implementation of the retrieval operator base.
 *
 * @param <T> gradoop temporal element tuple
 */
public class RetrievalOperatorBase<T extends TempElementTuple>
  implements FilterFunction<T> {
  /**
   * Serial.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Condition to be checked.
   */
  private final FourFunction<Long, Long, Long, Long, Boolean> condition;

  /**
   * From value to compare.
   */
  private final long from;

  /**
   * To value to compare.
   */
  private final long to;

  /**
   * Creates a OperatorBase instance with the parameters.
   *
   * @param condition the condition to be checked
   * @param from the from value to compare
   * @param to the to value to compare
   */
  public RetrievalOperatorBase(
    FourFunction<Long, Long, Long, Long, Boolean> condition, long from, long to) {
    Objects.requireNonNull(condition, "No condition was given.");

    this.condition = condition;
    this.from = from;
    this.to = to;
  }

  @Override
  public boolean filter(T element) throws Exception {
    boolean isIncluded = false;

    Long tFrom = element.getValidTime().f0;
    Long tTo = element.getValidTime().f1;

    if (condition.apply(from, to, tFrom, tTo)) {
      isIncluded = true;
    }

    return isIncluded;
  }
}
