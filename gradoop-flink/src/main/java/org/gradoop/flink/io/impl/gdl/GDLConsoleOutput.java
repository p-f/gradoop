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
package org.gradoop.flink.io.impl.gdl;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows to print graphs and graph collections to the standard output.
 */
public class GDLConsoleOutput {

  /**
   * Prints the logical graph to the standard output.
   *
   * @param logicalGraph The logical graph that is supposed to be printed.
   * @param <G>          type of the graph head
   * @param <V>          the vertex type
   * @param <E>          the edge type
   * @param <LG>         the type of the logical graph that will be created with this factory
   * @param <GC>         the type of the according graph collection
   * @throws Exception Forwarded from flink execute.
   */
  public static <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge, LG extends
    BaseGraph<G, V, E, LG, GC>, GC extends BaseGraphCollection<G, V, E, GC>> void print(
    BaseGraph<G, V, E, LG, GC> logicalGraph) throws Exception {
    print(logicalGraph.getCollectionFactory().fromGraph(logicalGraph));
  }

  /**
   * Prints the graph collection to the standard output.
   *
   * @param graphCollection The logical graph collection that is supposed to be printed.
   * @param <G>             type of the graph head
   * @param <V>             the vertex type
   * @param <E>             the edge type
   * @param <GC>            the type of the according graph collection
   * @throws Exception Forwarded from flink execute.
   */
  public static <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge, GC extends
    BaseGraphCollection<G, V, E, GC>> void print(
    BaseGraphCollection<G, V, E, GC> graphCollection) throws Exception {
    List<G> graphHeads = new ArrayList<>();
    graphCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(graphHeads));

    List<V> vertices = new ArrayList<>();
    graphCollection.getVertices().output(new LocalCollectionOutputFormat<>(vertices));

    List<E> edges = new ArrayList<>();
    graphCollection.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    graphCollection.getConfig().getExecutionEnvironment().execute();

    GDLEncoder encoder = new GDLEncoder<>(graphHeads, vertices, edges);
    String graphString = encoder.getGDLString();

    System.out.println(graphString);
  }
}
