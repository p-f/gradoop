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
package org.gradoop.flink.model.impl.functions.tpgm;

import org.gradoop.flink.model.impl.operators.tpgm.snapshot.FourFunction;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.RetrievalOperatorBase;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.RetrievalOperator;

/**
 * Implementation of the ValidDuring retrieval operator.
 */
public class ValidDuring extends RetrievalOperator {
  /**
   * Condition to be checked.
   */
  protected FourFunction<Long, Long, Long, Long, Boolean> condition =
  (from, to, tFrom, tTo) -> tFrom <= from && tTo >= to;

  /**
   * Creates a ValidDuring instance with the given time stamps.
   *
   * @param from the from value to compare
   * @param to the to value to compare
   */
  public ValidDuring(long from, long to) {
    this.vertexFilterFunction = new RetrievalOperatorBase<>(condition, from, to);
    this.edgeFilterFunction = new RetrievalOperatorBase<>(condition, from, to);
  }
}
