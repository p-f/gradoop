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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

/**
 * Reduce vertex tuples, calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
abstract class ReduceElementTuples<T extends Tuple> implements GroupReduceFunction<T, T> {

  /**
   * The data offset for tuples. Aggregate values are expected to start at this index.
   */
  protected final int tupleDataOffset;

  /**
   * The aggregate functions.
   */
  protected final List<AggregateFunction> aggregateFunctions;

  protected ReduceElementTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    this.aggregateFunctions = aggregateFunctions;
  }

  /**
   * Calculate aggregate functions and update tuple fields.
   *
   * @param superTuple       The tuple storing the current aggregate values.
   * @param inputTuple       The tuple storing the increment values.
   */
  protected void callAggregateFunctions(T superTuple, T inputTuple) {
    // Calculate aggregate values.
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final PropertyValue aggregate = superTuple.getField(i + tupleDataOffset);
      final PropertyValue increment = inputTuple.getField(i + tupleDataOffset);
      if (increment.equals(PropertyValue.NULL_VALUE)) {
        continue;
      } else if (aggregate.equals(PropertyValue.NULL_VALUE)) {
        superTuple.setField(increment, i + tupleDataOffset);
      } else {
        superTuple.setField(aggregateFunctions.get(i).aggregate(aggregate, increment), i + tupleDataOffset);
      }
    }
  }
}
