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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempVertexTuple;

/**
 * Defines the functions that are available on a retrieval operator.
 */
public abstract class RetrievalOperator {
  /**
   * FilterFunction to be applied on vertices.
   */
  protected RetrievalOperatorBase<TempVertexTuple> vertexFilterFunction;
  /**
   * FilterFunction to be applied on edges.
   */
  protected RetrievalOperatorBase<TempEdgeTuple> edgeFilterFunction;

  /**
   * Getter that returns the FilterFunction to be applied on vertices.
   *
   * @return FilterFunction for temporal vertices
   */
  public FilterFunction<TempVertexTuple> getVertexFilterFunction() {
    return vertexFilterFunction;
  }

  /**
   * Getter that returns the FilterFunction to be applied on edges.
   *
   * @return FilterFunction for temporal edges
   */
  public FilterFunction<TempEdgeTuple> getEdgeFilterFunction() {
    return edgeFilterFunction;
  }
}
