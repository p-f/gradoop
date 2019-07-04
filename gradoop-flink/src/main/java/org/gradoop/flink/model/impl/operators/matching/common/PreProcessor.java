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
package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.operators.matching.common.functions.BuildIdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.functions.BuildTripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingEdges;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingPairs;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingTriples;
import org.gradoop.flink.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithSourceEdgeCandidates;

/**
 * Provides methods for filtering vertices, edges, pairs (vertex + edge) and
 * triples based on a given query.
 */
public class PreProcessor {

  /**
   * Filters vertices based on the given GDL query. The resulting dataset only
   * contains vertex ids and their candidates that match at least one vertex in
   * the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching vertex ids and their candidates
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<IdWithCandidates<GradoopId>> filterVertices(
    LG graph, final String query) {
    return graph.getVertices()
      .filter(new MatchingVertices<>(query))
      .map(new BuildIdWithCandidates<>(query));
  }

  /**
   * Filters edges based on the given GDL query. The resulting dataset only
   * contains edges that match at least one edge in the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching edge triples and their candidates
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<TripleWithCandidates<GradoopId>> filterEdges(
    LG graph, final String query) {
    return graph.getEdges()
      .filter(new MatchingEdges<>(query))
      .map(new BuildTripleWithCandidates<>(query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph.
   *
   * @param g     data graph
   * @param query query graph
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching vertex-edge pairs
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<TripleWithSourceEdgeCandidates<GradoopId>>
    filterPairs(LG g, final String query) {
    return filterPairs(g, query, filterVertices(g, query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph and their corresponding candidates
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching vertex-edge pairs and their candidates
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<TripleWithSourceEdgeCandidates<GradoopId>>
    filterPairs(LG graph, final String query,
    DataSet<IdWithCandidates<GradoopId>> filteredVertices) {
    return filteredVertices
      .join(filterEdges(graph, query))
      .where(0).equalTo(1)
      .with(new MatchingPairs(query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching triples
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<TripleWithCandidates<GradoopId>> filterTriplets(
    LG graph, final String query) {
    return filterTriplets(graph, query, filterVertices(graph, query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The graph type.
   * @param <GC> The graph collection type.
   * @return dataset with matching triples and their candidates
   */
  public static <G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> DataSet<TripleWithCandidates<GradoopId>> filterTriplets(
    LG graph, final String query,
    DataSet<IdWithCandidates<GradoopId>> filteredVertices) {
    return filterPairs(graph, query, filteredVertices)
      .join(filteredVertices)
      .where(3).equalTo(0)
      .with(new MatchingTriples(query));
  }
}
