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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempEdgeTuple;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempVertexTuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test for the {@link ByTemporalPredicate} filter function.
 */
public class ByTemporalPredicateTest extends GradoopFlinkTestBase {

  /**
   * A temporal predicate used for testing.
   * Note that existing implementations of temporal predicates will be tested separately.
   */
  private final TemporalPredicate testPredicate = (from, to) -> from <= 2L && to > 5L;

  /**
   * A list of intervals to test. Those intervals should be accepted by the predicate.
   */
  private final List<Tuple2<Long, Long>> testIntervalsAccepted = Arrays.asList(
    Tuple2.of(MIN_VALUE, MAX_VALUE),
    Tuple2.of(1L, MAX_VALUE),
    Tuple2.of(2L, 6L),
    Tuple2.of(MIN_VALUE, 6L)
  );

  /**
   * A list of intervals to test. Those intervals should not be accepted by the predicate.
   */
  private final List<Tuple2<Long, Long>> testIntervalsOther = Arrays.asList(
    Tuple2.of(MIN_VALUE, 1L),
    Tuple2.of(2L, 4L),
    Tuple2.of(3L, MAX_VALUE),
    Tuple2.of(MIN_VALUE, 1L)
  );

  /**
   * Test the filter function on vertex tuples.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testForVertices() throws Exception {
    // Create vertex tuples for each accepted interval.
    List<TempVertexTuple> tuplesAccepted = testIntervalsAccepted.stream()
      .map(i -> new TempVertexTuple(GradoopId.get(), i)).collect(Collectors.toList());
    List<TempVertexTuple> inputTuples = new ArrayList<>(tuplesAccepted);
    // Create vertex tuples for other intervals.
    testIntervalsOther.stream().map(i -> new TempVertexTuple(GradoopId.get(), i))
      .forEach(inputTuples::add);
    // Apply the filter to the input.
    List<TempVertexTuple> result = getExecutionEnvironment()
      .fromCollection(inputTuples, TypeInformation.of(TempVertexTuple.class))
      .filter(new ByTemporalPredicate<>(testPredicate)).collect();
    // Sort the result and expected results to allow for comparison.
    Comparator<TempVertexTuple> comparator = Comparator.comparing(TempVertexTuple::getVertexId);
    result.sort(comparator);
    tuplesAccepted.sort(comparator);
    assertArrayEquals(tuplesAccepted.toArray(), result.toArray());
  }

  /**
   * Test the filter function on edge tuples.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testForEdges() throws Exception {
    // Create edge tuples for each accepted interval.
    List<TempEdgeTuple> tuplesAccepted = testIntervalsAccepted.stream()
      .map(i -> new TempEdgeTuple(GradoopId.get(), GradoopId.get(), GradoopId.get(), i))
      .collect(Collectors.toList());
    List<TempEdgeTuple> inputTuples = new ArrayList<>(tuplesAccepted);
    // Create edge tuples for other intervals.
    testIntervalsOther.stream()
      .map(i -> new TempEdgeTuple(GradoopId.get(), GradoopId.get(), GradoopId.get(), i))
      .forEach(inputTuples::add);
    // Apply the filter to the input.
    List<TempEdgeTuple> result = getExecutionEnvironment()
      .fromCollection(inputTuples, TypeInformation.of(TempEdgeTuple.class))
      .filter(new ByTemporalPredicate<>(testPredicate)).collect();
    // Sort the result and expected results to allow for comparison.
    Comparator<TempEdgeTuple> comparator = Comparator.comparing(TempEdgeTuple::getEdgeId);
    result.sort(comparator);
    tuplesAccepted.sort(comparator);
    assertArrayEquals(tuplesAccepted.toArray(), result.toArray());
  }
}
