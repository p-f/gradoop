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
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link TemporalVertexToTempVertexTuple} function.
 */
public class TemporalVertexToTempVertexTupleTest extends GradoopFlinkTestBase {

  /**
   * Test the map function. This test will map two test values and verify results.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testMap() throws Exception {
    EPGMVertexFactory<TemporalVertex> vf = getConfig().getTemporalGraphFactory().getVertexFactory();
    TemporalVertex test1 = vf.createVertex();
    test1.setValidFrom(1L);
    test1.setValidTo(2L);
    TemporalVertex test2 = vf.createVertex();
    test2.setValidFrom(3L);
    test2.setValidTo(4L);
    TempVertexTuple expected1 = new TempVertexTuple(test1.getId(), Tuple2.of(1L, 2L));
    TempVertexTuple expected2 = new TempVertexTuple(test2.getId(), Tuple2.of(3L, 4L));
    TemporalVertexToTempVertexTuple function = new TemporalVertexToTempVertexTuple();
    assertEquals(expected1, function.map(test1));
    assertEquals(expected2, function.map(test2));

    List<TempVertexTuple> result =
      getExecutionEnvironment().fromElements(test1, test2).map(function).collect();
    Comparator<TempVertexTuple> comparator = Comparator.comparing(TempVertexTuple::getVertexId);
    result.sort(comparator);
    List<TempVertexTuple> expected = Arrays.asList(expected1, expected2);
    expected.sort(comparator);
    assertArrayEquals(expected.toArray(), result.toArray());
  }
}
