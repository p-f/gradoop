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
package org.gradoop.flink.model.impl.operators.subgraph;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class TPGMSubgraphTest extends SubgraphTest {

  @Test
  public void testExistingSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("knows"))
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testExistingSubgraphWithVerification() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected =
      loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("knows"),
        Subgraph.Strategy.BOTH_VERIFIED)
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice),(bob),(carol),(dave),(eve),(frank)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .subgraph(
        v -> v.getLabel().equals("Person"),
        e -> e.getLabel().equals("friendOf"))
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEmptySubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.subgraph(
      v -> v.getLabel().equals("User"),
      e -> e.getLabel().equals("friendOf"))
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testVertexInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input
      .vertexInducedSubgraph(
        v -> v.getLabel().equals("Forum") || v.getLabel().equals("Tag"))
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.edgeInducedSubgraph(
      e -> e.getLabel().equals("hasTag")).toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraphProjectFirst() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph().toTemporalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    LogicalGraph output = input.subgraph(null,
      e -> e.getLabel().equals("hasTag"), Subgraph.Strategy.EDGE_INDUCED_PROJECT_FIRST)
      .toLogicalGraph();

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraphTemporal() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(gps)-[ghmd]->(dave)" +
      "(gdbs)-[ghma]->(alice)" +
      "(eve)-[ehid]->(databases)" +
      "]");

    TemporalGraph input = loader.getLogicalGraph()
      .toTemporalGraph(
        GradoopFlinkTestBase::extractGraphHeadTime,
        GradoopFlinkTestBase::extractVertexTime,
        GradoopFlinkTestBase::extractEdgeTime);

    // There are only 3 edges that are true for this predicate
    TemporalGraph tempOutput = input.edgeInducedSubgraph(
      e -> e.getValidTo() - e.getValidFrom() == 100000000L
    );

    LogicalGraph output = tempOutput.toLogicalGraph();

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(output.equalsByElementData(expected));
  }
}
