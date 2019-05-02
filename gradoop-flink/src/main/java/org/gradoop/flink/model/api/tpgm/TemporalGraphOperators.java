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
package org.gradoop.flink.model.api.tpgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.epgm.GraphBaseOperators;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.functions.tpgm.Between;
import org.gradoop.flink.model.impl.functions.tpgm.ContainedIn;
import org.gradoop.flink.model.impl.functions.tpgm.CreatedIn;
import org.gradoop.flink.model.impl.functions.tpgm.DeletedIn;
import org.gradoop.flink.model.impl.functions.tpgm.FromTo;
import org.gradoop.flink.model.impl.functions.tpgm.ValidDuring;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.Snapshot;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraphCollection;

import java.util.Objects;

/**
 * Defines the operators that are available on a {@link TemporalGraph}.
 */
public interface TemporalGraphOperators extends GraphBaseOperators {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Extracts a snapshot of this temporal graph using a given temporal predicate.
   * This will calculate the subgraph induced by the predicate.
   *
   * @param predicate the temporal predicate to apply
   * @return the snapshot as a new temporal graph
   */
  default TemporalGraph snapshot(TemporalPredicate predicate) {
    return callForGraph(new Snapshot(Objects.requireNonNull(predicate)));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate {@code AS OF timestamp}
   * where {@code timestamp} is a timestamp in milliseconds.
   *
   * @param timestamp the timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see AsOf
   */
  default TemporalGraph asOf(long timestamp) {
    return snapshot(new AsOf(timestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code FROM fromTimestamp TO toTimestamp} where both values are timestamps in milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see FromTo
   */
  default TemporalGraph fromTo(long fromTimestamp, long toTimestamp) {
    return snapshot(new FromTo(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code BETWEEN fromTimestamp AND toTimestamp} where both values are timestamps in
   * milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see Between
   */
  default TemporalGraph between(long fromTimestamp, long toTimestamp) {
    return snapshot(new Between(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code CONTAINED IN (fromTimestamp, toTimestamp)} where both values are timestamps in
   * milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see ContainedIn
   */
  default TemporalGraph containedIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new ContainedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code VALID DURING (fromTimestamp, toTimestamp)} where both values are timestamps in
   * milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see ValidDuring
   */
  default TemporalGraph validDuring(long fromTimestamp, long toTimestamp) {
    return snapshot(new ValidDuring(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code CREATED IN (fromTimestamp, toTimestamp)} where both values are timestamps in
   * milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see CreatedIn
   */
  default TemporalGraph createdIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new CreatedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Extracts a snapshot of this temporal graph using the temporal predicate
   * {@code DELETED IN (fromTimestamp, toTimestamp)} where both values are timestamps in
   * milliseconds.
   *
   * @param fromTimestamp the from timestamp in milliseconds to query
   * @param toTimestamp the to timestamp in milliseconds to query
   * @return the snapshot as a new temporal graph
   * @see Snapshot
   * @see DeletedIn
   */
  default TemporalGraph deletedIn(long fromTimestamp, long toTimestamp) {
    return snapshot(new DeletedIn(fromTimestamp, toTimestamp));
  }

  /**
   * Compares two snapshots of this graph. Given two temporal predicates, this operation
   * will check if a graph element (a vertex or an edge) was added, removed or kept in the snapshot
   * based on the second predicate.
   *
   * This operation returns the union of both snapshots of this, with the following changes:
   * A property with key {@value org.gradoop.flink.model.impl.operators.tpgm.diff.Diff#PROPERTY_KEY}
   * will be set on each graph element, its value will be set to
   * <ul>
   *   <li>{@code 0}, if the element is present in both snapshots.</li>
   *   <li>{@code 1}, if the element is present in the second, but not the first snapshot
   *   (i.e. it was added since the first snapshot).</li>
   *   <li>{@code -1}, if the element is present in the first, but not the second snapshot
   *   (i.e. it was removed since the first snapshot).</li>
   * </ul>
   * Graph elements present in neither snapshot will be discarded.
   * Dangling edges will be removed in an optional validation step.
   *
   * @param firstSnapshot  The predicate used to determine the first snapshot.
   * @param secondSnapshot The predicate used to determine the second snapshot.
   * @param validate       Should the graph (i.e. its edge set) be validated?
   * @return This graph with a
   * {@value org.gradoop.flink.model.impl.operators.tpgm.diff.Diff#PROPERTY_KEY} property set on all
   * elements present in one or both snapshots of this graph.
   */
  TemporalGraph diff(TemporalPredicate firstSnapshot, TemporalPredicate secondSnapshot,
    boolean validate);

  /**
   * Compares two snapshots of this graph with validation disabled.
   * This is equivalent to {@link #diff(TemporalPredicate, TemporalPredicate, boolean)
   * diff(firstSnapshot, secondSnapshot, false)}.
   *
   * @param firstSnapshot  The predicate used to determine the first snapshot.
   * @param secondSnapshot The predicate used to determine the second snapshot.
   * @return This graph with a
   * {@value org.gradoop.flink.model.impl.operators.tpgm.diff.Diff#PROPERTY_KEY} property set on
   * all elements present in one or both snapshots of this graph.
   * @see #diff(TemporalPredicate, TemporalPredicate, boolean) Description of this operator.
   */
  default TemporalGraph diff(TemporalPredicate firstSnapshot, TemporalPredicate secondSnapshot) {
    return diff(firstSnapshot, secondSnapshot, false);
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link TemporalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query) {
    return query(query, new GraphStatistics(1, 1, 1, 1));
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link TemporalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   *
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following
   * example:
   *
   * <pre>
   * <code>graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")
   * </code>
   * </pre>
   *
   * The query pattern is looking for pairs of authors that worked on the same paper. The
   * construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query Cypher query string
   * @param constructionPattern Construction pattern
   * @return graph collection containing the output of the construct pattern
   */
  default TemporalGraphCollection query(String query, String constructionPattern) {
    return query(query, constructionPattern, new GraphStatistics(1, 1, 1, 1));
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * @param query Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query, GraphStatistics graphStatistics) {
    return query(query, true, MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM,
      graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following
   * example:
   *
   * <pre>
   * <code>graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")
   * </code>
   * </pre>
   *
   * The query pattern is looking for pairs of authors that worked on the same paper. The
   * construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query Cypher query
   * @param constructionPattern Construction pattern
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing the output of the construct pattern
   */
  default TemporalGraphCollection query(String query, String constructionPattern,
    GraphStatistics graphStatistics) {
    return query(query, constructionPattern, true, MatchStrategy.HOMOMORPHISM,
      MatchStrategy.ISOMORPHISM, graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query Cypher query
   * @param attachData  attach original vertex and edge data to the result
   * @param vertexStrategy morphism setting for vertex mapping
   * @param edgeStrategy morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  default TemporalGraphCollection query(String query, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return query(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query Cypher query
   * @param constructionPattern Construction pattern
   * @param attachData  attach original vertex and edge data to the result
   * @param vertexStrategy morphism setting for vertex mapping
   * @param edgeStrategy morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  TemporalGraphCollection query(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the
   * given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  TemporalGraph vertexInducedSubgraph(FilterFunction<TemporalVertex> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given
   * filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  TemporalGraph edgeInducedSubgraph(FilterFunction<TemporalEdge> edgeFilterFunction);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction  vertex filter function
   * @param edgeFilterFunction    edge filter function
   * @return  logical graph which fulfils the given predicates and is a subgraph
   *          of that graph
   */
  default TemporalGraph subgraph(FilterFunction<TemporalVertex> vertexFilterFunction,
    FilterFunction<TemporalEdge> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return subgraph(vertexFilterFunction, edgeFilterFunction, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction  vertex filter function
   * @param edgeFilterFunction    edge filter function
   * @param strategy              execution strategy for the operator
   * @return  logical graph which fulfils the given predicates and is a subgraph
   *          of that graph
   */
  TemporalGraph subgraph(FilterFunction<TemporalVertex> vertexFilterFunction,
    FilterFunction<TemporalEdge> edgeFilterFunction, Subgraph.Strategy strategy);

  /**
   * Transforms the elements of the logical graph using the given transformation
   * functions. The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed logical graph
   */
  TemporalGraph transform(
    TransformationFunction<TemporalGraphHead> graphHeadTransformationFunction,
    TransformationFunction<TemporalVertex> vertexTransformationFunction,
    TransformationFunction<TemporalEdge> edgeTransformationFunction);

  /**
   * Transforms the graph head of the logical graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed logical graph
   */
  default TemporalGraph transformGraphHead(
    TransformationFunction<TemporalGraphHead> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  /**
   * Transforms the vertices of the logical graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed logical graph
   */
  default TemporalGraph transformVertices(
    TransformationFunction<TemporalVertex> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  /**
   * Transforms the edges of the logical graph using the given transformation
   * function. The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  default TemporalGraph transformEdges(
    TransformationFunction<TemporalEdge> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a temporal graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  TemporalGraph callForGraph(UnaryBaseGraphToBaseGraphOperator<TemporalGraph> operator);

  /**
   * Creates a graph collection from that graph using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  TemporalGraphCollection callForCollection(
    UnaryBaseGraphToBaseCollectionOperator<TemporalGraph, TemporalGraphCollection> operator);

  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------

  /**
   * Converts the {@link TemporalGraph} to a {@link LogicalGraph} instance by discarding all
   * temporal information from the graph elements. All Ids (graphs, vertices, edges) are kept
   * during the transformation.
   *
   * @return the logical graph instance
   */
  LogicalGraph toLogicalGraph();

}
