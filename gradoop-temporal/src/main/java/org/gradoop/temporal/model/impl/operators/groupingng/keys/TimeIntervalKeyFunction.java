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
package org.gradoop.temporal.model.impl.operators.groupingng.keys;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.temporal.model.api.functions.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;

/**
 * A key function extracting a {@link TimeDimension} from a {@link TemporalElement}.
 *
 * @param <T> The type of the temporal elements.
 */
public class TimeIntervalKeyFunction<T extends TemporalElement>
  implements KeyFunction<T, Tuple2<Long, Long>> {

  /**
   * The time interval to extract.
   */
  private final TimeDimension timeInterval;

  /**
   * Create a new instance of this key function.
   *
   * @param timeInterval The time interval to extract.
   */
  public TimeIntervalKeyFunction(TimeDimension timeInterval) {
    this.timeInterval = Objects.requireNonNull(timeInterval);
  }

  @Override
  public Tuple2<Long, Long> getKey(T element) {
    switch (timeInterval) {
    case VALID_TIME:
      return element.getValidTime();
    case TRANSACTION_TIME:
      return element.getTransactionTime();
    default:
      throw new UnsupportedOperationException("Time interval not supported by this element: " + timeInterval);
    }
  }

  @Override
  public void addKeyToElement(T element, Object key) {
    if (key instanceof Tuple2) {
      final Object firstElement = ((Tuple2) key).f0;
      final Object secondElement = ((Tuple2) key).f1;
      if ((firstElement instanceof Long) && (secondElement instanceof Long)) {
        switch (timeInterval) {
        case VALID_TIME:
          element.setValidFrom((Long) firstElement);
          element.setValidTo((Long) secondElement);
          break;
        case TRANSACTION_TIME:
          element.setTransactionTime(Tuple2.of((Long) firstElement, (Long) secondElement));
          break;
        default:
        }
        return;
      } else {
        throw new IllegalArgumentException("Invalid types for tuple key: " +
          firstElement.getClass().getSimpleName() + ", " + secondElement.getClass().getSimpleName());
      }
    } else {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
  }

  @Override
  public TypeInformation<Tuple2<Long, Long>> getType() {
    return TupleTypeInfo.getBasicTupleTypeInfo(Long.TYPE, Long.TYPE);
  }

  @Override
  public String toString() {
    return timeInterval.toString();
  }
}
