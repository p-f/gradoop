/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.impl.operators.distinction.functions.CountGraphHeads;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class GroupByIsomorphismTest extends DistinctByIsomorphismTestBase {

  @Test
  public void execute() throws Exception {
    GraphCollection collection = getTestCollection();

    String propertyKey = "count";

    GraphHeadReduceFunction<EPGMGraphHead> countFunc = new CountGraphHeads<>(propertyKey);

    collection = collection.groupByIsomorphism(countFunc);

    List<EPGMGraphHead> graphHeads = collection.getGraphHeads().collect();

    assertEquals(3, graphHeads.size());

    for (EPGMGraphHead graphHead : graphHeads) {
      assertTrue(graphHead.hasProperty(propertyKey));
      int count = graphHead.getPropertyValue(propertyKey).getInt();

      String label = graphHead.getLabel();

      if (label.equals("G")) {
        assertEquals(1, count);
      } else {
        assertEquals(2, count);
      }
    }
  }
}
