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
package org.gradoop.flink.model.impl.operators.tpgm.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

import java.util.List;
import java.util.Objects;

/**
 * Build a tuple-based representation of elements for grouping.
 * Tuples will contain some Gradoop IDs, all grouping keys followed by all properties to be
 * aggregated.
 * <p>
 * <i>Note: </i> This function sets all grouping keys and aggregate values, make sure to set
 * additional
 * fields, if {@code tupleDataOffset} is not {@code 0}.
 *
 * @param <E> The element type.
 */
public class BuildTuplesFromElements<E extends EPGMElement>
  implements MapFunction<E, Tuple>, ResultTypeQueryable<Tuple> {

  /**
   * The grouping key functions.
   */
  private final List<GroupingKeyFunction<? super E, ?>> keys;

  /**
   * The aggregate functions.
   */
  private final List<AggregateFunction> aggregateFunctions;

  /**
   * The number of fields to be reserved for IDs.
   * Those fields will be of type {@link GradoopId}.
   */
  private final int tupleDataOffset;

  /**
   * The types of the produced tuple.
   */
  private final Class<?>[] elementTypes;

  /**
   * Reduce object instantiations.
   */
  private final Tuple reuseTuple;

  /**
   * Initialize this function, setting the grouping keys and aggregate functions.
   *
   * @param tupleDataOffset    The number of tuple fields reserved for IDs.
   * @param keys               The grouping keys.
   * @param aggregateFunctions The aggregate functions used to determine the aggregate property
   */
  public BuildTuplesFromElements(int tupleDataOffset, List<GroupingKeyFunction<? super E, ?>> keys,
    List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    if (tupleDataOffset < 0) {
      throw new IllegalArgumentException(
        "The number of reserved tuple fields must not be negative.");
    }
    this.keys = Objects.requireNonNull(keys);
    this.aggregateFunctions = Objects.requireNonNull(aggregateFunctions);
    final int tupleSize = tupleDataOffset + keys.size() + aggregateFunctions.size();
    if (tupleSize > Tuple.MAX_ARITY) {
      throw new UnsupportedOperationException("Number of elements is too high for tuple: " +
        tupleSize);
    }
    elementTypes = new Class[tupleSize];
    for (int i = 0; i < tupleDataOffset; i++) {
      elementTypes[i] = GradoopId.class;
    }
    // Fill grouping key types.
    for (int i = 0; i < keys.size(); i++) {
      elementTypes[i + tupleDataOffset] = keys.get(i).getType();
    }
    // Fill remaining spots with property value types.
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      elementTypes[i + keys.size() + tupleDataOffset] = PropertyValue.class;
    }
    reuseTuple = Tuple.newInstance(tupleSize);
    // Fill first fields with default ID values.
    for (int i = 0; i < tupleDataOffset; i++) {
      reuseTuple.setField(GradoopId.NULL_VALUE, i);
    }
  }

  @Override
  public TypeInformation<Tuple> getProducedType() {
    TypeInformation<?>[] componentTypes = new TypeInformation[elementTypes.length];
    for (int i = 0; i < componentTypes.length; i++) {
      componentTypes[i] = TypeInformation.of(elementTypes[i]);
    }
    return new TupleTypeInfo<>(componentTypes);
  }

  @Override
  public Tuple map(E element) throws Exception {
    int field = tupleDataOffset;
    for (GroupingKeyFunction<? super E, ?> key : keys) {
      reuseTuple.setField(key.getKey(element), field);
      field++;
    }
    for (AggregateFunction aggregateFunction : aggregateFunctions) {
      reuseTuple.setField(aggregateFunction.getIncrement(element), field);
      field++;
    }
    return reuseTuple;
  }
}
