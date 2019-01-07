/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot;

import java.io.Serializable;

/**
 * Defines the functional interface used to store the condition of retrieval operators.
 *
 * @param <One> type of first function argument
 * @param <Two> type of second function argument
 * @param <Three> type of third function argument
 * @param <Four> type of fourth function argument
 * @param <Return> type of the function result
 */
@FunctionalInterface
public interface FourFunction<One, Two, Three, Four, Return> extends Serializable {
  /**
   * Applies this function to the given arguments.
   *
   * @param one the first function argument
   * @param two the second function argument
   * @param three the third function argument
   * @param four the fourth function argument
   * @return the function result
   */
  Return apply(One one, Two two, Three three, Four four);
}
