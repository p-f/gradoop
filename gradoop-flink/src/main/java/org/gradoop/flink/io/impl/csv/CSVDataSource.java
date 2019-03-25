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
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToElement;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToTemporalEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToTemporalGraphHead;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToTemporalVertex;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data source for CSV files.
 * <p>
 * The datasource expects files separated by vertices and edges, e.g. in the following directory
 * structure:
 * <p>
 * csvRoot
 * |- vertices.csv # all vertex data
 * |- edges.csv    # all edge data
 * |- graphs.csv   # all graph head data
 * |- metadata.csv # Meta data for all data contained in the graph
 */
public class CSVDataSource extends CSVBase implements DataSource {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config  Gradoop Flink configuration
   */
  public CSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Graph heads will be disposed at the moment. The following issue attempts to provide
   * alternatives to keep graph heads: https://github.com/dbs-leipzig/gradoop/issues/974
   */
  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    return getCollection(new CSVLineToGraphHead(getConfig().getGraphHeadFactory()),
      new CSVLineToVertex(getConfig().getVertexFactory()),
      new CSVLineToEdge(getConfig().getEdgeFactory()), getConfig().getGraphCollectionFactory());
  }

  @Override
  public TemporalGraph getTemporalGraph() {
    TemporalGraphCollection temporalGraphCollection = getTemporalGraphCollection();
    return getConfig().getTemporalGraphFactory()
      .fromDataSets(temporalGraphCollection.getVertices(), temporalGraphCollection.getEdges());
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() {
    return getCollection(new CSVLineToTemporalGraphHead(getConfig().getTemporalGraphHeadFactory()),
      new CSVLineToTemporalVertex(getConfig().getTemporalVertexFactory()),
      new CSVLineToTemporalEdge(getConfig().getTemporalEdgeFactory()),
      getConfig().getTemporalGraphCollectionFactory());
  }

  /**
   * Creates a graph collection using the given mapper functions and collection factory.
   *
   * @param csvToGraphHeadMap a mapping function to create a {@link EPGMGraphHead} from a CSV line
   * @param csvToVertexMap a mapping function to create a {@link EPGMVertex} from a CSV line
   * @param csvToEdgeMap a mapping function to create a {@link EPGMEdge} from a CSV line
   * @param graphCollectionFactory the factory that is responsible for creating a graph collection
   * @param <GC> the type of the graph collection
   * @param <G> the type of the {@link EPGMGraphHead}
   * @param <V> the type of the {@link EPGMVertex}
   * @param <E> the type of the {@link EPGMEdge}
   * @return a graph collection of type {@link GC} representing the graph stored as CSV
   */
  private <GC extends BaseGraphCollection<G, V, E, GC>, G extends EPGMGraphHead, V extends
    EPGMVertex, E extends EPGMEdge> GC getCollection(
    CSVLineToElement<G> csvToGraphHeadMap,
    CSVLineToElement<V> csvToVertexMap,
    CSVLineToElement<E> csvToEdgeMap,
    BaseGraphCollectionFactory<G, V, E, GC> graphCollectionFactory) {

    // Read the meta data
    DataSet<Tuple3<String, String, String>> metaData =
      new CSVMetaDataSource().readDistributed(getMetaDataPath(), getConfig());

    // Read the datasets of each graph element
    DataSet<G> graphHeads = getConfig().getExecutionEnvironment()
      .readTextFile(getGraphHeadCSVPath())
      .map(csvToGraphHeadMap).withBroadcastSet(metaData, BC_METADATA);

    DataSet<V> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(csvToVertexMap).withBroadcastSet(metaData, BC_METADATA);

    DataSet<E> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(csvToEdgeMap).withBroadcastSet(metaData, BC_METADATA);

    // Create the graph
    return graphCollectionFactory.fromDataSets(graphHeads, vertices, edges);
  }
}
