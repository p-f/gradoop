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
package org.gradoop.flink.model.impl.operators.statistics.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Set;

/**
 * Extracts Triples of the form {@code <Tuple1<PropertyName>, PropertyValue>} from the given list of
 * GraphElements
 *
 * @param <T> graph element type
 */
public class ExtractPropertyValues<T extends EPGMGraphElement>
  implements FlatMapFunction<T, Tuple2<String, Set<PropertyValue>>> {

  /**
   * Reuse Tuple
   */
  private final Tuple2<String, Set<PropertyValue>> reuseTuple;

  /**
   * Creates a new UDF
   */
  public ExtractPropertyValues() {
    this.reuseTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(T value, Collector<Tuple2<String, Set<PropertyValue>>> out) throws Exception {
    if (value.getProperties() != null) {
      for (Property property : value.getProperties()) {
        reuseTuple.f0 = property.getKey();
        reuseTuple.f1 = Sets.newHashSet(property.getValue());

        out.collect(reuseTuple);
      }
    }
  }
}
