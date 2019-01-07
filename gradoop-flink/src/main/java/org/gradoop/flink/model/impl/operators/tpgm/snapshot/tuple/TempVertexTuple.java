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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (vertexId, validTime)
 */
public class TempVertexTuple extends Tuple2<GradoopId, Tuple2<Long, Long>>
  implements TempElementTuple {

  /**
   * serial
   */
  private static final long serialVersionUID = 42L;

  /**
   * default constructor
   */
  public TempVertexTuple() {
  }

  /**
   * constructor with field values
   *
   * @param vertexId vertex id
   * @param validTime valid time interval
   */
  public TempVertexTuple(GradoopId vertexId, Tuple2<Long, Long> validTime) {
    this.f0 = vertexId;
    this.f1 = validTime;
  }

  /**
   * Get the gradoop id of the vertex.
   *
   * @return the gradoop id of the vertex
   */
  public GradoopId getVertexId() {
    return f0;
  }

  public Tuple2<Long, Long> getValidTime() {
    return f1;
  }
}
