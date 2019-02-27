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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link TemporalEdgeToTempEdgeTuple} function.
 */
public class TemporalEdgeToTempEdgeTupleTest extends GradoopFlinkTestBase {
  /**
   * Test the map function. This test will map two test values and verify results.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testMap() throws Exception {
    EPGMEdgeFactory<TemporalEdge> ef = getConfig().getTemporalGraphFactory().getEdgeFactory();
    GradoopId test1 = GradoopId.get();
    GradoopId test2 = GradoopId.get();
    TemporalEdge edge1 = ef.createEdge(test1, test2);
    edge1.setValidFrom(1L);
    edge1.setValidTo(2L);
    TemporalEdge edge2 = ef.createEdge(test2, test1);
    edge2.setValidFrom(3L);
    edge2.setValidTo(4L);
    TemporalEdgeToTempEdgeTuple function = new TemporalEdgeToTempEdgeTuple();
    TempEdgeTuple expected1 =
      new TempEdgeTuple(edge1.getId(), edge1.getSourceId(), edge1.getTargetId(), Tuple2.of(1L, 2L));
    TempEdgeTuple expected2 =
      new TempEdgeTuple(edge2.getId(), edge2.getSourceId(), edge2.getTargetId(), Tuple2.of(3L, 4L));
    assertEquals(expected1, function.map(edge1));
    assertEquals(expected2, function.map(edge2));

    List<TempEdgeTuple> result =
      getExecutionEnvironment().fromElements(edge1, edge2).map(function).collect();
    Comparator<TempEdgeTuple> comparator = Comparator.comparing(TempEdgeTuple::getEdgeId);
    result.sort(comparator);
    List<TempEdgeTuple> expected = Arrays.asList(expected1, expected2);
    expected.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
