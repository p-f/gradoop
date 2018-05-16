/**
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
package org.gradoop.flink.io.impl.json.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test if the {@link EntityToJSON} class encodes all possible {@link PropertyValue} types
 * correctly.
 */
public class VertexToJSONTest extends GradoopFlinkTestBase {

  /**
   * The output encoder to test.
   */
  private VertexToJSON<Vertex> encoder;

  /**
   * The input decoder to test.
   */
  private JSONToVertex decoder;

  /**
   * The vertex to encode/decode.
   */
  private Vertex testVertex;

  /**
   * The factory used to create the vertex.
   */
  private VertexFactory vertexFactory;

  /**
   * The property key to use for testing.
   */
  private static final String key = "testkey";

  @Before
  public void setUp() {
    vertexFactory = getConfig().getVertexFactory();
    testVertex = vertexFactory.createVertex();
    encoder = new VertexToJSON<>();
    decoder = new JSONToVertex(vertexFactory);
  }

  @After
  public void tearDown() {
    testVertex = null;
  }

  @Test
  public void testFormatNull() throws Exception {
    runTestForType(null);
  }

  @Test
  public void testFormatBoolean() throws Exception {
    runTestForType(Boolean.TRUE);
  }

  @Test
  public void testFormatInteger() throws Exception {
    // Test using some integer.
    runTestForType(123456);
  }

  @Test
  public void testFormatLong() throws Exception {
    // Test using some long, making sure it does not fit in an integer.
    runTestForType(10L + (long) Integer.MAX_VALUE);
  }

  @Test
  public void testFormatFloat() throws Exception {
    runTestForType(1234.6789f);
  }

  @Test
  public void testFormatDouble() throws Exception {
    runTestForType(123456.0789d);
  }

  @Test
  public void testFormatString() throws Exception {
    runTestForType("Some test String.");
  }

  @Test
  public void testFormatBigDecimal() throws Exception {
    // Make sure the BigDecimal is larger than Long.MAX_VALUE
    runTestForType(BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(10L)));
  }

  @Test
  public void testFormatGradoopId() throws Exception {
    runTestForType(GradoopId.get());
  }

  @Test
  public void testFormatMap() throws Exception {
    // Create a Map with some test values.
    Map<Object, Object> map = new HashMap<>();
    map.put(PropertyValue.create("Key 1"), PropertyValue.create(1));
    map.put(PropertyValue.create("Key 2"), PropertyValue.create(1.1f));
    map.put(PropertyValue.create("Key 3"), PropertyValue.create("Value"));
    map.put(PropertyValue.create("Key 4"), PropertyValue.create(100L));
    runTestForType(map);
  }

  @Test
  public void testFormatList() throws Exception {
    // Create a List with some test values.
    List<PropertyValue> list = new ArrayList<>();
    list.add(PropertyValue.create(1));
    list.add(PropertyValue.create(1.1f));
    list.add(PropertyValue.create("Test String"));
    list.add(PropertyValue.create(100L));
    runTestForType(list);
  }

  @Test
  public void testFormatDate() throws Exception {
    runTestForType(LocalDate.now());
  }

  @Test
  public void testFormatTime() throws Exception {
    runTestForType(LocalTime.now());
  }

  @Test
  public void testFormatDateTime() throws Exception {
    runTestForType(LocalDateTime.now());
  }

  private void runTestForType(Object value) throws Exception {
    testVertex.setProperty(key, PropertyValue.create(value));
    String formated = encoder.format(testVertex);
    Vertex decoded = decoder.map(formated);
    assertEquals(testVertex, decoded);
    assertEquals(testVertex.getProperties(), decoded.getProperties());
  }
}