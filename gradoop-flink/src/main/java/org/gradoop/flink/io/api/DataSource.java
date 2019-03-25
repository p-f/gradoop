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
public interface DataSource {

  /**
   * Reads the input as logical graph.
   *
   * @return logial graph
   * @throws IOException if the reading of the graph data fails
   */
  LogicalGraph getLogicalGraph() throws IOException;

  /**
   * Reads the input as graph collection.
   *
   * @return graph collection
   * @throws IOException if the reading of the graph data fails
   */
  GraphCollection getGraphCollection() throws IOException;

  /**
   * Reads the input as temporal graph.
   *
   * By default, an {@link UnsupportedOperationException} is thrown, if the implementing source
   * doesn't overwrite this method.
   *
   * @return temporal graph
   * @throws IOException if the reading of the graph data fails
   */
  default TemporalGraph getTemporalGraph() throws IOException {
    throw new UnsupportedOperationException(
      "Reading a temporal graph with this source is not supported yet.");
  }

  /**
   * Reads the input as temporal graph collection.
   *
   * By default, an {@link UnsupportedOperationException} is thrown, if the implementing source
   * doesn't overwrite this method.
   *
   * @return temporal graph collection
   * @throws IOException if the reading of the graph data fails
   */
  default TemporalGraphCollection getTemporalGraphCollection() throws IOException {
    throw new UnsupportedOperationException(
      "Reading a temporal graph collection with this source is not supported yet."
    );
  }
}
