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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.functions.tpgm.From;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.functions.Extractor;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SnapshotTest extends GradoopFlinkTestBase {

  @Test
  public void testAsOf() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();
    LogicalGraph lg = loader.getLogicalGraphByVariable("g0");

    TemporalGraph tg = toTemporalGraph(lg);

    //expected
    loader.initDatabaseFromString("expected: [" +
      "(v_Person_0:Person {gender:\"f\",city:\"Dresden\",__valFrom:1543800000000L,speaks:\"English\",name:\"Eve\",age:35})" +
      "(v_Person_1:Person {gender:\"f\",city:\"Leipzig\",__valFrom:1543400000000L,name:\"Alice\",age:20})" +
      "(v_Person_2:Person {gender:\"m\",city:\"Leipzig\",__valFrom:1543500000000L,name:\"Bob\",age:30})" +
      "(v_Person_0)-[e_knows_0:knows{__valFrom:1543800000000L,since:2013}]->(v_Person_1)" +
      "(v_Person_1)-[e_knows_1:knows{__valFrom:1543600000000L,since:2014}]->(v_Person_2)" +
      "(v_Person_2)-[e_knows_2:knows{__valFrom:1543600000000L,since:2014}]->(v_Person_1)" +
      "]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    TemporalGraph output = tg.callForGraph(new Snapshot(new AsOf(1543800000000L)));

    collectAndAssertTrue(output.toLogicalGraph().equalsByElementData(expected));
  }

  @Test
  public void testFrom() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();
    LogicalGraph lg = loader.getLogicalGraphByVariable("g0");

    TemporalGraph tg = toTemporalGraph(lg);

    //expected
    loader.initDatabaseFromString("expected: [" +
      "]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    TemporalGraph output = tg.callForGraph(new Snapshot(new From(1543300000000L, 1544400000000L)));

    output.print();

    collectAndAssertTrue(output.toLogicalGraph().equalsByElementData(expected));
  }

  private TemporalGraph toTemporalGraph(LogicalGraph lg) {
    TemporalGraph tg = getConfig().getTemporalGraphFactory().fromNonTemporalDataSets(
      lg.getGraphHead(),
      new Extractor<>(),
      lg.getVertices(),
      new Extractor<>(),
      lg.getEdges(),
      new Extractor<>());

    return tg;
  }
}
