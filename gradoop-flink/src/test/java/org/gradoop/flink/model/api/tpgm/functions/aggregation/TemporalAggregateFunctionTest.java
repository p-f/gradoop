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
package org.gradoop.flink.model.api.tpgm.functions.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Test for the {@link TemporalAggregateFunction} interface, checking if temporal elements are
 * handled correctly.
 */
public class TemporalAggregateFunctionTest extends GradoopFlinkTestBase {
  /**
   * A temporal aggregate function used for this test.
   * This functions returns the validFrom time.
   */
  private TemporalVertexAggregateFunction function = new TemporalVertexAggregateFunction() {
    @Override
    public PropertyValue getIncrement(TemporalElement element) {
      return PropertyValue.create(element.getValidFrom());
    }

    @Override
    public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
      return null;
    }

    @Override
    public String getAggregatePropertyKey() {
      return null;
    }
  };

  /**
   * Test if {@link TemporalAggregateFunction} handles temporal elements correctly.
   */
  @Test
  public void testWithTemporal() {
    TemporalVertex vertex = getConfig().getTemporalVertexFactory().createVertex();
    vertex.setValidTime(Tuple2.of(2L, 3L));
    assertEquals(PropertyValue.create(2L), function.getIncrement(vertex));
  }

  /**
   * Test if {@link TemporalAggregateFunction} handles non-temporal elements correctly.
   * (In this case an exception should be thrown, as there is no non-temporal default value set.)
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testWithNonTemporal() {
    Vertex vertex = getConfig().getVertexFactory().createVertex();
    function.getIncrement(vertex);
  }

  /**
   * Test if {@link TemporalAggregateFunction} handles non-temporal elements correctly when
   * a non-temporal default value is set.
   */
  @Test
  public void testWithNonTemporalAndDefaultValue() {
    TemporalVertexAggregateFunction withDefault = spy(function);
    // Do not call the real method, return some default value instead.
    doAnswer(i -> PropertyValue.create(0L)).when(withDefault).getNonTemporalDefaultValue(any());
    Vertex vertex = getConfig().getVertexFactory().createVertex();
    assertEquals(PropertyValue.create(0L), withDefault.getIncrement(vertex));
  }
}
