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
package org.gradoop.flink.model.impl.operators.tpgm.diff.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.operators.tpgm.diff.Diff;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Test for the {@link DiffPerElement} map function.
 */
public class DiffPerElementTest extends GradoopFlinkTestBase {

  /**
   * A temporal predicate accepting all ranges.
   */
  private static final TemporalPredicate ALL = (from, to) -> true;

  /**
   * A temporal predicate accepting no ranges.
   */
  private static final TemporalPredicate NONE = (from, to) -> false;

  /**
   * A time interval set on test elements.
   * (The value is ignored, as the predicates {@link #ALL} and {@link #NONE} don't check times.)
   */
  private static final Tuple2<Long, Long> TEST_TIME = Tuple2.of(0L, 0L);

  /**
   * Test the map function using an edge.
   *
   * @throws Exception when the flap map function throws an exception.
   */
  @Test
  public void testWithEdges() throws Exception {
    EPGMEdgeFactory<TemporalEdge> edgeFactory =
      getConfig().getTemporalGraphFactory().getEdgeFactory();
    runTestForElement(() -> edgeFactory.createEdge(GradoopId.get(), GradoopId.get()));
  }

  /**
   * Test the map function on a dataset of edges.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithEdgesInDataSets() throws Exception {
    EPGMEdgeFactory<TemporalEdge> edgeFactory =
      getConfig().getTemporalGraphFactory().getEdgeFactory();
    runTestOnDataSet(() -> edgeFactory.createEdge(GradoopId.get(), GradoopId.get()));
  }

  /**
   * Test the map function using a vertex.
   *
   * @throws Exception when the flat map function throws an exception.
   */
  @Test
  public void testWithVertices() throws Exception {
    EPGMVertexFactory<TemporalVertex> vertexFactory =
      getConfig().getTemporalGraphFactory().getVertexFactory();
    runTestForElement(vertexFactory::createVertex);
  }

  /**
   * Test the map function on a dataset of vertices.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithVerticesInDataSets() throws Exception {
    EPGMVertexFactory<TemporalVertex> vertexFactory =
      getConfig().getTemporalGraphFactory().getVertexFactory();
    runTestOnDataSet(vertexFactory::createVertex);
  }

  /**
   * Run a {@link FlatMapFunction} on a single elements, store it's results in a {@link List}.
   * This is a helper function used get the result of a flat map operation.
   *
   * @param function The function to test.
   * @param input    The value to be used as the input for that function.
   * @param <E>      The argument and result type of the function.
   * @return         A list of results of the flat map operation.
   * @throws Exception when the flat map operation throws an exception.
   */
  private <E> List<E> runFlatMapFunction(FlatMapFunction<E, E> function, E input) throws Exception {
    List<E> results = new ArrayList<>();
    function.flatMap(input, new ListCollector<>(results));
    return results;
  }

  /**
   * Test the map function on some test elements.
   * This will try all possible outcomes of the diff and check if the property value is set
   * accordingly.
   *
   * @param elementFactory A supplier used to create the test elements.
   * @param <E> The temporal element type to test the map function on.
   * @throws Exception When the flat map operation throws an exception.
   */
  private <E extends TemporalElement> void runTestForElement(Supplier<E> elementFactory) throws
    Exception {
    // The element will be present in both snapshots, it is "equal".
    E testElement = elementFactory.get();
    testElement.setValidTime(TEST_TIME);
    List<E> results = runFlatMapFunction(new DiffPerElement<>(ALL, ALL), testElement);
    assertEquals(1, results.size());
    E result = results.get(0);
    assertSame(testElement, result); // The function should return the same instance.
    assertEquals(Diff.VALUE_EQUAL, result.getPropertyValue(Diff.PROPERTY_KEY));

    // The element will only be present in the first snapshot, it is "removed".
    testElement = elementFactory.get();
    testElement.setValidTime(TEST_TIME);
    results = runFlatMapFunction(new DiffPerElement<>(ALL, NONE), testElement);
    assertEquals(1, results.size());
    result = results.get(0);
    assertSame(testElement, result);
    assertEquals(Diff.VALUE_REMOVED, result.getPropertyValue(Diff.PROPERTY_KEY));

    // The element will only be present in the second snapshot, it is "added".
    testElement = elementFactory.get();
    testElement.setValidTime(TEST_TIME);
    results = runFlatMapFunction(new DiffPerElement<>(NONE, ALL), testElement);
    assertEquals(1, results.size());
    result = results.get(0);
    assertSame(testElement, result);
    assertEquals(Diff.VALUE_ADDED, result.getPropertyValue(Diff.PROPERTY_KEY));

    // The element will be present in neither snapshot, it should be removed.
    testElement = elementFactory.get();
    testElement.setValidTime(TEST_TIME);
    results = runFlatMapFunction(new DiffPerElement<>(NONE, NONE), testElement);
    assertEquals(0, results.size());
  }

  /**
   * Test the map function by applying it on a dataset.
   *
   * @param elementFactory A supplier used to create the test elements.
   * @param <E> The temporal element type to test the map function on.
   * @throws Exception when the execution in Flink fails.
   */
  private <E extends TemporalElement> void runTestOnDataSet(Supplier<E> elementFactory)
    throws Exception {
    // A predicate used to check if a range was valid before 0 (unix timestamp).
    TemporalPredicate beforeEpoch = (from, to) -> from < 0;
    // A predicate used to check if a range was valid after 0 (unix timestamp).
    TemporalPredicate afterEpoch = (from, to) -> to > 0;
    // Create some test elements. Those elements are either
    // 1. never
    // 2. only before epoch
    // 3. only after epoch
    // 4. before and after epoch
    // A label is set on each of those elements to distinguish it later.
    E neverValid = elementFactory.get();
    neverValid.setLabel("never");
    neverValid.setValidTime(Tuple2.of(0L, 0L));
    E validBeforeEpoch = elementFactory.get();
    validBeforeEpoch.setLabel("before");
    validBeforeEpoch.setValidTime(Tuple2.of(-1L, 0L));
    E validAfterEpoch = elementFactory.get();
    validAfterEpoch.setLabel("after");
    validAfterEpoch.setValidTime(Tuple2.of(0L, 1L));
    E validInBoth = elementFactory.get();
    validInBoth.setLabel("both");
    validInBoth.setValidTime(Tuple2.of(-1L, 1L));
    // A diff is called, comparing the dataset before and after epoch.
    List<E> result = getExecutionEnvironment()
      .fromElements(neverValid, validAfterEpoch, validBeforeEpoch, validInBoth)
      .flatMap(new DiffPerElement<E>(beforeEpoch, afterEpoch)).collect();
    assertEquals(3, result.size());
    assertEquals(3, result.stream().map(Element::getLabel).distinct().count());
    // Check if each of the test elements has the correct property value set.
    for (E resultElement : result) {
      switch (resultElement.getLabel()) {
      case "never":
        fail("The element not matching either predicate should have been removed.");
        break;
      case "both":
        assertEquals(Diff.VALUE_EQUAL, resultElement.getPropertyValue(Diff.PROPERTY_KEY));
        break;
      case "before":
        assertEquals(Diff.VALUE_REMOVED, resultElement.getPropertyValue(Diff.PROPERTY_KEY));
        break;
      case "after":
        assertEquals(Diff.VALUE_ADDED, resultElement.getPropertyValue(Diff.PROPERTY_KEY));
        break;
      default:
        fail("Unknown label.");
      }
    }
  }
}
