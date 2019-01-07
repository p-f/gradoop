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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempVertexTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TemporalEdgeToTempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TemporalVertexToTempVertexTuple;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

/**
 * Extracts a snapshot of a temporal graph using the given retrieval operator.
 */
public class Snapshot implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {

  /**
   * Used retrieval operator.
   */
  private final RetrievalOperator retrievalOperator;

  /**
   * Creates an instance of the snapshot operator with the given retrieval operator.
   *
   * @param retrievalOperator the retrieval operator to be used
   */
  public Snapshot(RetrievalOperator retrievalOperator) {
    Objects.requireNonNull(retrievalOperator, "No retrieval operator was given.");

    this.retrievalOperator = retrievalOperator;
  }

  @Override
  public TemporalGraph execute(TemporalGraph superGraph) {
    return subgraphAndVerify(superGraph);
  }

  /**
   * Returns the subgraph of the given temporal graph that is defined by the
   * vertices that fulfil the vertex filter function and edges that fulfill
   * the edge filter function.
   *
   * Note, that the operator does verify the consistency of the resulting
   * graph.
   *
   * @param superGraph supergraph
   * @return subgraph
   */
  private TemporalGraph subgraphAndVerify(TemporalGraph superGraph) {
    DataSet<TempVertexTuple> vertices =
      superGraph.getVertices()
      .map(new TemporalVertexToTempVertexTuple())
      .filter(retrievalOperator.getVertexFilterFunction());
    DataSet<TempEdgeTuple> edges =
      superGraph.getEdges().map(new TemporalEdgeToTempEdgeTuple())
      .filter(retrievalOperator.getEdgeFilterFunction());

    DataSet<Tuple2<Tuple2<TempEdgeTuple, TempVertexTuple>, TempVertexTuple>> verifiedTriples =
      edges
      .join(vertices)
      .where("0.f1").equalTo("1.f0")
      .join(vertices)
      .where("0.f2").equalTo("1.f0");

    DataSet<TempEdgeTuple> verifiedEdges = verifiedTriples
      .map(new Value0Of2<>())
      .map(new Value0Of2<>());

    DataSet<TempVertexTuple> verifiedVertices = verifiedTriples
      .map(new Value0Of2<>())
      .map(new Value1Of2<>())
      .union(verifiedTriples.map(new Value1Of2<>()))
      .distinct("f0");

    return buildGraph(superGraph, verifiedVertices, verifiedEdges);
  }

  /**
   * Joins the given TempVertexTuple and TempEdgeTuple DataSets with the vertex and egde DataSets
   * of the original graph and returns a TemporalGraph object.
   *
   * @param superGraph supergraph
   * @param vertices filtered vertices
   * @param edges filtered edges
   * @return subgraph
   */
  private TemporalGraph buildGraph(TemporalGraph superGraph, DataSet<TempVertexTuple> vertices,
    DataSet<TempEdgeTuple> edges) {
    DataSet<TemporalVertex> originalVertices = superGraph.getVertices();
    DataSet<TemporalEdge> originalEdges = superGraph.getEdges();

    DataSet<TemporalVertex> joinedVertices =
      originalVertices
      .joinWithTiny(vertices)
      .where("0.id").equalTo("1.f0")
      .map(new Value0Of2<>());

    DataSet<TemporalEdge> joinedEdges =
      originalEdges
      .joinWithTiny(edges)
      .where("0.id").equalTo("1.f0")
      .map(new Value0Of2<>());

    return superGraph.getFactory().fromDataSets(joinedVertices, joinedEdges);
  }

  @Override
  public String getName() {
    return Snapshot.class.getName();
  }
}
