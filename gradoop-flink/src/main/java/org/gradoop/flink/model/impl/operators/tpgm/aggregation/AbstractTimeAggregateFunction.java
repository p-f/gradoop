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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.api.tpgm.functions.aggregation.TemporalAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

import java.util.Objects;

/**
 * An abstract super class for aggregation functions that aggregate a time of a temporal element.
 * Times can be a {@link TemporalAttribute.Field} of a {@link TemporalAttribute}.
 */
public abstract class AbstractTimeAggregateFunction extends BaseAggregateFunction
  implements TemporalAggregateFunction {

  /**
   * Selects which time-interval is considered by this aggregate function.
   */
  private final TemporalAttribute interval;

  /**
   * Selects the field of the temporal element to consider.
   */
  private final TemporalAttribute.Field field;

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param interval             The time-interval to consider.
   * @param field                The field of the time-interval to consider.
   */
  public AbstractTimeAggregateFunction(String aggregatePropertyKey, TemporalAttribute interval,
    TemporalAttribute.Field field) {
    super(aggregatePropertyKey);
    this.interval = Objects.requireNonNull(interval);
    this.field = Objects.requireNonNull(field);
  }

  /**
   * Get a time stamp as the aggregate value from a temporal element.
   * The value will be the value of a {@link TemporalAttribute.Field} of a
   * {@link TemporalAttribute}.
   *
   * @param element The temporal element.
   * @return The value, as a long-type property value.
   */
  @Override
  public PropertyValue getIncrement(TemporalElement element) {
    final Tuple2<Long, Long> timeInterval;
    switch (interval) {
    case TRANSACTION_TIME:
      timeInterval = element.getTransactionTime();
      break;
    case VALID_TIME:
      timeInterval = element.getValidTime();
      break;
    default:
      throw new IllegalArgumentException("Temporal attribute " + interval + " is not supported " +
        "by this aggregate function.");
    }
    switch (field) {
    case FROM:
      return PropertyValue.create(timeInterval.f0);
    case TO:
      return PropertyValue.create(timeInterval.f1);
    default:
      throw new IllegalArgumentException("Field " + field + " is not supported for time intervals" +
        ".");
    }
  }

  @Override
  public String toString() {
    return String.format("%s(%s.%s)", getClass().getSimpleName(), interval, field);
  }
}
