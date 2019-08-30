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
package org.gradoop.flink.model.impl.operators.groupingng.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.GroupingKeyFunction;
import org.gradoop.flink.model.impl.operators.groupingng.keys.LabelKeyFunction;

import java.util.List;
import java.util.Objects;

/**
 * Build the final super-elements from the internal tuple-based representation.
 *
 * @param <T> The input tuple type.
 * @param <E> The final element type.
 */
abstract class BuildSuperElementFromTuple<T extends Tuple, E extends Element>
  implements MapFunction<T, E>, ResultTypeQueryable<E> {

  /**
   * The data offset for tuples. Grouping keys and aggregate values are expected to start at this
   * index.
   */
  private final int tupleDataOffset;

  /**
   * The grouping key functions..
   */
  private final List<GroupingKeyFunction<E, ?>> keyFunctions;

  /**
   * The aggregate functions for this element type.
   */
  private final List<AggregateFunction> aggregateFunctions;

  /**
   * Initialize this function.
   *
   * @param tupleDataOffset    The number of reserved fields in the tuple.
   * @param groupingKeys       The grouping key functions.
   * @param aggregateFunctions The aggregate functions.
   */
  protected BuildSuperElementFromTuple(int tupleDataOffset,
    List<GroupingKeyFunction<E, ?>> groupingKeys,
    List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    this.keyFunctions = Objects.requireNonNull(groupingKeys);
    this.aggregateFunctions = Objects.requireNonNull(aggregateFunctions);
  }

  /**
   * Calculate the final aggregate values by calling post-processing functions and set them
   * to the element. Also set grouping keys as properties on the element.
   *
   * @param element   The element to store the properties.
   * @param tupleData The internal tuple-based representation of the element.
   * @return The final element with all set properties.
   */
  protected E setAggregatePropertiesAndKeys(E element, T tupleData) {
    // Set grouping keys.
    for (int i = 0; i < keyFunctions.size(); i++) {
      final Object groupingKey = tupleData.getField(tupleDataOffset + i);
      element = keyFunctions.get(i).setAsProperty(element, groupingKey);
    }
    // Calculate aggregate values and set them.
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final AggregateFunction function = aggregateFunctions.get(i);
      element.setProperty(function.getAggregatePropertyKey(),
        function.postAggregate(tupleData.getField(tupleDataOffset + keyFunctions.size() + i)));
    }
    return element;
  }
}
