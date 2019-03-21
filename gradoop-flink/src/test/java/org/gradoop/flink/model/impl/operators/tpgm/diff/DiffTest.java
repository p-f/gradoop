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
package org.gradoop.flink.model.impl.operators.tpgm.diff;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for the temporal diff operator.
 */
public class DiffTest extends GradoopFlinkTestBase {
  /**
   * Test the temporal diff operator on an example graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testDiffOnGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();
    loader.appendToDatabaseFromString("expected[" +
      "(gpse:Forum {title: \"Graph Processing\", _diff: 0})" +
      "(davee:Person {name: \"Dave\", gender: \"m\", city: \"Dresden\", age: 40," +
      "__valFrom: 1543700000000L, _diff: 1})" +
      "(carole:Person {name : \"Carol\", gender : \"f\", city : \"Dresden\", age : 30," +
      "__valFrom: 1543600000000L, _diff: 0})" +
      "(gpse)-[:hasMember{ __valFrom: 1543600000000L, __valTo: 1543800000000L, _diff: -1}]->" +
      "(davee) (gpse)-[:hasMember {_diff: 0}]->(carole)" +
      "-[:knows {since : 2014 , __valFrom : 1543700000000L, _diff: 1}]->(davee)" +
      "]");
    LogicalGraph inputGraphEpgm = loader.getLogicalGraphByVariable("g3");
    TemporalGraph temporalGraph = toTemporalGraph(inputGraphEpgm);
    TemporalGraph result = temporalGraph.diff(new AsOf(1543600000000L), new AsOf(1543800000000L));
    collectAndAssertTrue(loader.getLogicalGraphByVariable("expected").equalsByData(
      result.toLogicalGraph()));
  }

  /**
   * Test the temporal diff operator with validation.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testDiffOnGraphWithValidate() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("testGraph[" +
      "(a:A {__valFrom: 0L, __valTo: 3L})-[:e]->(b:B {__valFrom: 2L, __valTo: 5L})" +
      "] expected1 [" +
      "(a1:A {__valFrom: 0L, __valTo: 3L, _diff: 0})" +
      "] expected2 [" +
      "(b2:B {__valFrom: 2L, __valTo: 5L, _diff: 0})" +
      "]");
    // We need to verify the actual vertex/edge sets, as equals ignores dangling edges.
    // The dangling edge with label e should be removed with a validate step.
    TemporalGraph result1 = toTemporalGraph(loader.getLogicalGraphByVariable("testGraph"))
      .diff(new AsOf(0L), new AsOf(1L), true);
    List<TemporalVertex> vertices = new ArrayList<>();
    List<TemporalEdge> edges = new ArrayList<>();
    result1.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    result1.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    assertEquals(1, vertices.size());
    assertEquals(0, edges.size());
    assertEquals("A", vertices.get(0).getLabel());
    // Without validate the dangling edge should not be removed, as it matches the temporal
    // predicate.
    vertices.clear();
    edges.clear();
    result1 = toTemporalGraph(loader.getLogicalGraphByVariable("testGraph"))
      .diff(new AsOf(0L), new AsOf(1L), false);
    result1.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    result1.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    assertEquals(1, vertices.size());
    assertEquals(1, edges.size());
    assertEquals("A", vertices.get(0).getLabel());
    // Run the same tests, except with to opposite edge direction.
    TemporalGraph result2 = toTemporalGraph(loader.getLogicalGraphByVariable("testGraph"))
      .diff(new AsOf(3L), new AsOf(4L), true);
    vertices.clear();
    edges.clear();
    result2.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    result2.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    assertEquals(1, vertices.size());
    assertEquals(0, edges.size());
    assertEquals("B", vertices.get(0).getLabel());
    // Again, without validate.
    result2 = toTemporalGraph(loader.getLogicalGraphByVariable("testGraph"))
      .diff(new AsOf(3L), new AsOf(4L), false);
    vertices.clear();
    edges.clear();
    result2.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    result2.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    assertEquals(1, vertices.size());
    assertEquals(1, edges.size());
    assertEquals("B", vertices.get(0).getLabel());
  }

  /**
   * Transform a logical graph to a temporal graph.
   *
   * @param graph The logical graph.
   * @return The temporal graph.
   */
  private TemporalGraph toTemporalGraph(LogicalGraph graph) {
    return graph.toTemporalGraph(GradoopFlinkTestBase::extractGraphHeadTime,
      GradoopFlinkTestBase::extractVertexTime, GradoopFlinkTestBase::extractEdgeTime);
  }
}
