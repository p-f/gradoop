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
 * Implementation of the AsOf retrieval operator.
 */
public class AsOf extends RetrievalOperator {
  /**
   * Condition to be checked.
   */
  protected FourFunction<Long, Long, Long, Long, Boolean> condition =
  (from, to, tFrom, tTo) -> tFrom <= from && tTo <= from;

  /**
   * Creates a AsOf instance with the given time stamp.
   *
   * @param timestamp the time stamp to compare
   */
  public AsOf(long timestamp) {
    this.vertexFilterFunction = new RetrievalOperatorBase<>(condition, timestamp, 0);
    this.edgeFilterFunction = new RetrievalOperatorBase<>(condition, timestamp, 0);
  }
}
