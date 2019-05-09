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

import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;

/**
 * Abstract base class for calculating the maximum of a field of a time-interval for temporal
 * elements. This function ignores the default value ({@link Long#MAX_VALUE}) and handles
 * it the same way as {@code null}.
 */
public class MaxTime extends AbstractTimeAggregateFunction {

  /**
   * The property value that is considered the default value of this aggregate function.
   * This value is ignored during aggregation.
   */
  private static final PropertyValue DEFAULT =
    PropertyValue.create(TemporalElement.DEFAULT_TIME_TO);

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param interval             The time-interval to consider.
   * @param field                The field of the time-interval to consider.
   */
  public MaxTime(String aggregatePropertyKey, TemporalAttribute interval,
    TemporalAttribute.Field field) {
    super(aggregatePropertyKey, interval, field);
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    if (aggregate.isNull() || aggregate.equals(DEFAULT)) {
      return increment.equals(DEFAULT) ? PropertyValue.NULL_VALUE : increment;
    } else if (increment.isNull() || increment.equals(DEFAULT)) {
      return aggregate;
    } else {
      return PropertyValueUtils.Numeric.max(aggregate, increment);
    }
  }
}
