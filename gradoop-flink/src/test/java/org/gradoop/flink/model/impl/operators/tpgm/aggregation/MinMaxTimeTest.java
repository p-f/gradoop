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
package org.gradoop.flink.model.impl.operators.tpgm.aggregation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute.Field.FROM;
import static org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute.Field.TO;
import static org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute.TRANSACTION_TIME;
import static org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute.VALID_TIME;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link MinTime} and {@link MaxTime} (by testing the respective vertex- and
 * edge-aggregations).
 */
@RunWith(Parameterized.class)
public class MinMaxTimeTest extends GradoopFlinkTestBase {

  /**
   * A factory used to create some test edges.
   */
  private EPGMEdgeFactory<TemporalEdge> edgeFactory = getConfig().getTemporalEdgeFactory();

  /**
   * A factory used to create some test vertices.
   */
  private EPGMVertexFactory<TemporalVertex> vertexFactory = getConfig().getTemporalVertexFactory();

  /**
   * The temporal attribute to aggregate.
   */
  @Parameterized.Parameter
  public TemporalAttribute temporalAttribute;

  /**
   * The field of the temporal attribute to aggregate.
   */
  @Parameterized.Parameter(1)
  public TemporalAttribute.Field field;

  /**
   * The expected value for the {@link MaxEdgeTime} function.
   */
  @Parameterized.Parameter(2)
  public Long expectedMaxEdge;

  /**
   * The expected value for the {@link MinEdgeTime} function.
   */
  @Parameterized.Parameter(3)
  public Long expectedMinEdge;

  /**
   * The expected value for the {@link MaxVertexTime} function.
   */
  @Parameterized.Parameter(4)
  public Long expectedMaxVertex;

  /**
   * The expected value for the {@link MinVertexTime} function.
   */
  @Parameterized.Parameter(5)
  public Long expectedMinVertex;

  /**
   * The expected value for the {@link MaxTime} function.
   */
  @Parameterized.Parameter(6)
  public Long expectedMax;

  /**
   * The expected value for the {@link MinTime} function.
   */
  @Parameterized.Parameter(7)
  public Long expectedMin;

  /**
   * Test all {@link MinTime} and {@link MaxTime} related aggregate functions.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationFunctions() throws Exception {
    final String keyMaxEdge = "maxEdgeTime";
    final String keyMinEdge = "minEdgeTime";
    final String keyMaxVertex = "maxVertexTime";
    final String keyMinVertex = "minVertexTime";
    final String keyMax = "maxTime";
    final String keyMin = "minTime";
    TemporalGraph result = getTestGraphWithValues().aggregate(
      new MaxEdgeTime(keyMaxEdge, temporalAttribute, field),
      new MinEdgeTime(keyMinEdge, temporalAttribute, field),
      new MaxVertexTime(keyMaxVertex, temporalAttribute, field),
      new MinVertexTime(keyMinVertex, temporalAttribute, field),
      new MinTime(keyMin, temporalAttribute, field),
      new MaxTime(keyMax, temporalAttribute, field));
    TemporalGraphHead head = result.getGraphHead().collect().get(0);
    assertEquals(PropertyValue.create(expectedMaxEdge), head.getPropertyValue(keyMaxEdge));
    assertEquals(PropertyValue.create(expectedMinEdge), head.getPropertyValue(keyMinEdge));
    assertEquals(PropertyValue.create(expectedMaxVertex), head.getPropertyValue(keyMaxVertex));
    assertEquals(PropertyValue.create(expectedMinVertex), head.getPropertyValue(keyMinVertex));
    assertEquals(PropertyValue.create(expectedMax), head.getPropertyValue(keyMax));
    assertEquals(PropertyValue.create(expectedMin), head.getPropertyValue(keyMin));
  }

  /**
   * Test all {@link MinTime} and {@link MaxTime} related aggregate function where all
   * temporal temporal attributes are set to default values.
   * This will check if the aggregate values are null, when all of the values are set to
   * the default value.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationFunctionsWithAllDefaults() throws Exception {
    final String keyMaxEdge = "maxEdgeTime";
    final String keyMinEdge = "minEdgeTime";
    final String keyMaxVertex = "maxVertexTime";
    final String keyMinVertex = "minVertexTime";
    final String keyMax = "maxTime";
    final String keyMin = "minTime";
    TemporalGraph result = getTestGraphWithAllDefaults().aggregate(
      new MaxEdgeTime(keyMaxEdge, temporalAttribute, field),
      new MinEdgeTime(keyMinEdge, temporalAttribute, field),
      new MaxVertexTime(keyMaxVertex, temporalAttribute, field),
      new MinVertexTime(keyMinVertex, temporalAttribute, field),
      new MinTime(keyMin, temporalAttribute, field),
      new MaxTime(keyMax, temporalAttribute, field));
    TemporalGraphHead head = result.getGraphHead().collect().get(0);
    // The expected values for max and min aggregations. Those should be null, when the minimum
    // of all FROM or the maximum of all TO fields is calculated.
    final PropertyValue min = field == FROM ? PropertyValue.NULL_VALUE :
      PropertyValue.create(MAX_VALUE);
    final PropertyValue max = field == TO ? PropertyValue.NULL_VALUE :
      PropertyValue.create(MIN_VALUE);
    assertEquals(max, head.getPropertyValue(keyMaxEdge));
    assertEquals(min, head.getPropertyValue(keyMinEdge));
    assertEquals(max, head.getPropertyValue(keyMaxVertex));
    assertEquals(min, head.getPropertyValue(keyMinVertex));
    assertEquals(max, head.getPropertyValue(keyMax));
    assertEquals(min, head.getPropertyValue(keyMin));
  }

  /**
   * Get parameters for this test. Those are
   * <ol>
   * <li>The {@link TemporalAttribute} to aggregate.</li>
   * <li>The {@link TemporalAttribute.Field} of that attribute to aggregate.</li>
   * <li>The expected result of {@link MaxEdgeTime}.</li>
   * <li>The expected result of {@link MinEdgeTime}.</li>
   * <li>The expected result of {@link MaxVertexTime}.</li>
   * <li>The expected result of {@link MinVertexTime}.</li>
   * <li>The expected result of {@link MaxTime}.</li>
   * <li>The expected result of {@link MinTime}.</li>
   * </ol>
   *
   * @return The parameters for this test.
   */
  @Parameterized.Parameters(name = "{0}.{1}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {TRANSACTION_TIME, FROM, 4L, -2L, 3L, 0L, 4L, -2L},
      {TRANSACTION_TIME, TO, 5L, -2L, 3L, 1L, 5L, -2L},
      {VALID_TIME, FROM, 7L, -1L, 4L, 1L, 7L, -1L},
      {VALID_TIME, TO, 6L, -1L, 4L, 1L, 6L, -1L}
    });
  }

  /**
   * Get a test graph with temporal attributes set.
   *
   * @return The test graph.
   */
  private TemporalGraph getTestGraphWithValues() {
    TemporalVertex v1 = createVertex(MIN_VALUE, 1, MIN_VALUE, 1);
    TemporalVertex v2 = createVertex(0, MAX_VALUE, 1, MAX_VALUE);
    TemporalVertex v3 = createVertex(1, 2, 3, 4);
    TemporalVertex v4 = createVertex(2, 1, 4, 3);
    TemporalVertex v5 = createVertex(3, 3, 1, 2);
    TemporalEdge e1 = createEdge(v1, v2, MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalEdge e2 = createEdge(v2, v3, -1, -2, 7, 6);
    TemporalEdge e3 = createEdge(v4, v5, -2, 0, 1, 1);
    TemporalEdge e4 = createEdge(v3, v5, 4, 5, -1, -1);
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2, v3, v4, v5);
    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3, e4);
    return getConfig().getTemporalGraphFactory().fromDataSets(vertices, edges);
  }

  /**
   * Get a test graph with all temporal attributes set to their default value.
   *
   * @return The test graph.
   */
  private TemporalGraph getTestGraphWithAllDefaults() {
    TemporalVertex v1 = createVertex(MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalVertex v2 = createVertex(MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalVertex v3 = createVertex(MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalEdge e1 = createEdge(v1, v2, MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalEdge e2 = createEdge(v2, v3, MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    TemporalEdge e3 = createEdge(v3, v1, MIN_VALUE, MAX_VALUE, MIN_VALUE, MAX_VALUE);
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2, v3);
    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3);
    return getConfig().getTemporalGraphFactory().fromDataSets(vertices, edges);
  }

  /**
   * Create a temporal edge with temporal attributes set.
   *
   * @param source    The element used as a source for the edge.
   * @param target    The element used as a target for the edge.
   * @param txFrom    The start of the transaction time.
   * @param txTo      The end of the transaction time.
   * @param validFrom The start of the valid time.
   * @param validTo   The end of the valid time.
   * @return A temporal edge with those times set.
   */
  private TemporalEdge createEdge(EPGMIdentifiable source, EPGMIdentifiable target, long txFrom,
    long txTo, long validFrom, long validTo) {
    TemporalEdge edge = edgeFactory.createEdge(source.getId(), target.getId());
    edge.setTransactionTime(Tuple2.of(txFrom, txTo));
    edge.setValidTime(Tuple2.of(validFrom, validTo));
    return edge;
  }

  /**
   * Create a temporal vertex with temporal attributes set.
   *
   * @param txFrom    The start of the transaction time.
   * @param txTo      The end of the transaction time.
   * @param validFrom The start of the valid time.
   * @param validTo   The end of the valid time.
   * @return A temporal vertex with those times set.
   */
  private TemporalVertex createVertex(long txFrom, long txTo, long validFrom, long validTo) {
    TemporalVertex vertex = vertexFactory.createVertex();
    vertex.setTransactionTime(Tuple2.of(txFrom, txTo));
    vertex.setValidTime(Tuple2.of(validFrom, validTo));
    return vertex;
  }
}
