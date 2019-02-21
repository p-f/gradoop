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
    return subgraphAndVerify(superGraph);
  }

  /**
   * Returns the subgraph of the given temporal graph that is induced by the temporal predicate.
   * <p>
   * Note, that the operator does verify the consistency of the resulting graph.
   *
   * @param superGraph The input graph.
   * @return subgraph The resulting graph.
   */
  private TemporalGraph subgraphAndVerify(TemporalGraph superGraph) {
    DataSet<TempVertexTuple> vertices = superGraph.getVertices()
      .map(new TemporalVertexToTempVertexTuple())
      .filter(new ByTemporalPredicate<>(temporalPredicate));
    DataSet<TempEdgeTuple> edges = superGraph.getEdges().map(new TemporalEdgeToTempEdgeTuple())
      .filter(new ByTemporalPredicate<>(temporalPredicate));

    DataSet<TempEdgeTuple> verifiedEdges = edges
      .join(vertices).where(1).equalTo(0).with(new LeftSide<>())
      .join(vertices).where(2).equalTo(0).with(new LeftSide<>());

    return buildGraph(superGraph, vertices, verifiedEdges);
  }

  /**
   * Joins the given TempVertexTuple and TempEdgeTuple DataSets with the vertex and egde DataSets
   * of the original graph and returns a TemporalGraph object.
   *
   * @param superGraph The original graph.
   * @param vertices   The filtered vertices.
   * @param edges      The filtered and verified edges.
   * @return subgraph  A graph from the original graph containing only the filtered elements.
   */
  private TemporalGraph buildGraph(TemporalGraph superGraph, DataSet<TempVertexTuple> vertices,
    DataSet<TempEdgeTuple> edges) {
    DataSet<TemporalVertex> originalVertices = superGraph.getVertices();
    DataSet<TemporalEdge> originalEdges = superGraph.getEdges();

    DataSet<TemporalVertex> joinedVertices =
      originalVertices.join(vertices).where(new Id<>()).equalTo(0).with(new LeftSide<>());

    DataSet<TemporalEdge> joinedEdges =
      originalEdges.join(edges).where(new Id<>()).equalTo(0).with(new LeftSide<>());

    return superGraph.getFactory().fromDataSets(joinedVertices, joinedEdges);
  }
}
