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
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (edgeId, sourceVertexId, targetVertexId, validTime)
 */
public class TempEdgeTuple extends Tuple4<GradoopId, GradoopId, GradoopId, Tuple2<Long, Long>>
  implements TempElementTuple {

  /**
   * serial
   */
  private static final long serialVersionUID = 42L;

  /**
   * default constructor
   */
  public TempEdgeTuple() {
  }

  /**
   * constructor with field values
   *
   * @param edgeId edge id
   * @param sourceVertexId id of the source vertex
   * @param targetVertexId id of the target vertex
   * @param validTime valid time interval
   */
  public TempEdgeTuple(GradoopId edgeId, GradoopId sourceVertexId, GradoopId targetVertexId,
    Tuple2<Long, Long> validTime) {
    this.f0 = edgeId;
    this.f1 = sourceVertexId;
    this.f2 = targetVertexId;
    this.f3 = validTime;
  }

  /**
   * Get the gradoop id of the edge.
   *
   * @return the gradoop id of the edge
   */
  public GradoopId getEdgeId() {
    return f0;
  }

  /**
   * Get the gradoop id of the source vertex.
   *
   * @return the gradoop id of the source vertex
   */
  public GradoopId getSourceVertexId() {
    return f1;
  }

  /**
   * Get the gradoop id of the target vertex.
   *
   * @return the gradoop id of the target vertex
   */
  public GradoopId getTargetVertexId() {
    return f2;
  }

  public Tuple2<Long, Long> getValidTime() {
    return f3;
  }
}
