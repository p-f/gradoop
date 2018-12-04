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
package org.gradoop.common.model.impl.pojo.temporal;

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

import java.io.Serializable;
import java.util.Objects;

/**
 * Factory for creating temporal edge POJOs.
 */
public class TemporalEdgeFactory implements EPGMEdgeFactory<TemporalEdge>, Serializable {

  @Override
  public TemporalEdge createEdge(GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(id, GradoopConstants.DEFAULT_EDGE_LABEL, sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, null);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, properties);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties) {
    return initEdge(id, label, sourceVertexId, targetVertexId, properties, null);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, graphIds);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphIds);
  }

  @Override
  public TemporalEdge createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    Properties properties, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  @Override
  public TemporalEdge initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Properties properties, GradoopIdSet graphIds) {
    return new TemporalEdge(
      Objects.requireNonNull(id, "Identifier is null."),
      Objects.requireNonNull(label, "Label is null."),
      Objects.requireNonNull(sourceVertexId, "Source vertex id was null."),
      Objects.requireNonNull(targetVertexId, "Target vertex id was null."),
      properties,
      graphIds,
      null,
      null
    );
  }

  @Override
  public Class<TemporalEdge> getType() {
    return TemporalEdge.class;
  }
}
