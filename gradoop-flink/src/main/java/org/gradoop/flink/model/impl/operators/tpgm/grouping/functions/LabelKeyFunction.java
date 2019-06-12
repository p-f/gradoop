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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

/**
 * A grouping key function extracting the label.
 *
 * @param <T> The type of the elements to group.
 */
public class LabelKeyFunction<T extends EPGMLabeled> implements GroupingKeyFunction<T, String> {

  /**
   * A property key used to indicate that the value should not actually be stored as a property,
   * but as the label instead.
   */
  public static final String LABEL_KEY = ":label";

  @Override
  public String getKey(T element) {
    return element.getLabel();
  }

  @Override
  public String getTargetPropertyKey() {
    return LABEL_KEY;
  }

  @Override
  public TypeInformation<String> getType() {
    return TypeInformation.of(String.class);
  }
}
