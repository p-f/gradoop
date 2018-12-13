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
package org.gradoop.flink.model.impl.operators.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateIdEquality;

public class TPGMTransformationTest extends TransformationTest {

  @Test
  public void testIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(testGraphString);

    List<GradoopId> expectedGraphHeadIds = Lists.newArrayList();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();

    TemporalGraph inputGraph = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    inputGraph.getGraphHead().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputGraph.getVertices().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputGraph.getEdges().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    LogicalGraph result = inputGraph
      .transform(
        TransformationTest::transformGraphHead,
        TransformationTest::transformVertex,
        TransformationTest::transformEdge)
      .toLogicalGraph();

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds = Lists.newArrayList();
    List<GradoopId> resultEdgeIds = Lists.newArrayList();

    result.getGraphHead()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    result.getVertices()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultVertexIds));
    result.getEdges()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    validateIdEquality(expectedGraphHeadIds, resultGraphHeadIds);
    validateIdEquality(expectedVertexIds, resultVertexIds);
    validateIdEquality(expectedEdgeIds, resultEdgeIds);
  }

  /**
   * Tests the data in the resulting graph.
   *
   * @throws Exception on failure
   */
  @Test
  public void testDataEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(testGraphString);

    TemporalGraph original = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("g01");

    LogicalGraph result = original
      .transform(
        TransformationTest::transformGraphHead,
        TransformationTest::transformVertex,
        TransformationTest::transformEdge)
      .toLogicalGraph();

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testGraphHeadOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(testGraphString);

    TemporalGraph original = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("g02");

    LogicalGraph result = original
      .transformGraphHead(TransformationTest::transformGraphHead)
      .toLogicalGraph();

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testVertexOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(testGraphString);

    TemporalGraph original = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("g03");

    LogicalGraph result = original
      .transformVertices(TransformationTest::transformVertex)
      .toLogicalGraph();

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(testGraphString);

    TemporalGraph original = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("g04");

    LogicalGraph result = original
      .transformEdges(TransformationTest::transformEdge)
      .toLogicalGraph();

    collectAndAssertTrue(result.equalsByData(expected));
  }
}
