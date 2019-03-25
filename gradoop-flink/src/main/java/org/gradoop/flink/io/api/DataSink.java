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
package org.gradoop.flink.io.api;

import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraphCollection;

import java.io.IOException;

/**
 * Data source in analytical programs.
 */
public interface DataSink {

  /**
   * Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   * @throws IOException if the writing of the graph data fails
   */
  void write(LogicalGraph logicalGraph) throws IOException;

  /**
   * Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   * @throws IOException if the writing of the graph data fails
   */
  void write(GraphCollection graphCollection) throws IOException;

  /**
   * Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException;

  /**
   * Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  void write(GraphCollection graphCollection, boolean overwrite) throws IOException;

  /**
   * Writes a temporal graph to the data sink.
   *
   * @param temporalGraph temporal graph
   * @throws IOException if the writing of the graph data fails
   */
  default void write(TemporalGraph temporalGraph) throws IOException {
    write(temporalGraph, false);
  }

  /**
   * Writes a temporal graph collection to the data sink.
   *
   * @param temporalGraphCollection temporal graph collection
   * @throws IOException if the writing of the graph data fails
   */
  default void write(TemporalGraphCollection temporalGraphCollection) throws IOException {
    write(temporalGraphCollection, false);
  }

  /**
   * Writes a temporal graph to the data sink.
   *
   * By default, an {@link UnsupportedOperationException} is thrown, if the implementing sink
   * doesn't overwrite this method.
   *
   * @param temporalGraph temporal graph
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  default void write(TemporalGraph temporalGraph, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException(
      "Writing a temporal graph with this sink is not supported yet.");
  }

  /**
   * Writes a temporal graph collection to the data sink.
   *
   * By default, an {@link UnsupportedOperationException} is thrown, if the implementing sink
   * doesn't overwrite this method.
   *
   * @param temporalGraphCollection temporal graph collection
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  default void write(TemporalGraphCollection temporalGraphCollection, boolean overwrite)
    throws IOException {
    throw new UnsupportedOperationException(
      "Writing a temporal graph collection with this sink is not supported yet.");
  }
}
