/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempVertexTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TemporalEdgeToTempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TemporalVertexToTempVertexTuple;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

import java.util.Objects;

/**
 * Extracts a snapshot of a temporal graph using a given temporal predicate.
 * This will calculate the subgraph of a temporal graph induced by the predicate.
 * The resulting graph will be verified, i.e. dangling edges will be removed.
 */
public class Snapshot implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {

  /**
   * Used temporal predicate.
   */
  private final TemporalPredicate temporalPredicate;

  /**
   * Creates an instance of the snapshot operator with the given temporal predicate.
   *
   * @param predicate The temporal predicate.
   */
  public Snapshot(TemporalPredicate predicate) {
    temporalPredicate = Objects.requireNonNull(predicate, "No predicate was given.");
  }

  @Override
  public TemporalGraph execute(TemporalGraph superGraph) {
    DataSet<TempVertexTuple> vertices = superGraph.getVertices()
      .map(new TemporalVertexToTempVertexTuple())
      .filter(new ByTemporalPredicate<>(temporalPredicate));
    DataSet<TempEdgeTuple> edges = superGraph.getEdges().map(new TemporalEdgeToTempEdgeTuple())
      .filter(new ByTemporalPredicate<>(temporalPredicate));

    DataSet<TempEdgeTuple> verifiedEdges = edges
      .join(vertices).where(1).equalTo(0).with(new LeftSide<>())
      .join(vertices).where(2).equalTo(0).with(new LeftSide<>());

    DataSet<TemporalVertex> originalVertices = superGraph.getVertices();
    DataSet<TemporalEdge> originalEdges = superGraph.getEdges();

    DataSet<TemporalVertex> resultVertices =
      originalVertices.join(vertices).where(new Id<>()).equalTo(0).with(new LeftSide<>());

    DataSet<TemporalEdge> resultEdges =
      originalEdges.join(verifiedEdges).where(new Id<>()).equalTo(0).with(new LeftSide<>());

    return superGraph.getFactory().fromDataSets(resultVertices, resultEdges);
  }

}
