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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Abstract class representing an EPGM element that is contained in a logical
 * graph (i.e. vertices and edges).
 */
public abstract class EPGMGraphElement extends EPGMElement implements GraphElement {

  /**
   * Set of graph identifiers that element is contained in
   */
  private GradoopIdSet graphIds;

  /**
   * Default constructor.
   */
  protected EPGMGraphElement() {
  }

  /**
   * Creates an EPGM graph element using the given arguments.
   *  @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphIds     graphIds that element is contained in
   */
  protected EPGMGraphElement(GradoopId id, String label,
    Properties properties, GradoopIdSet graphIds) {
    super(id, label, properties);
    this.graphIds = graphIds;
  }

  @Override
  public GradoopIdSet getGraphIds() {
    return graphIds;
  }

  @Override
  public void addGraphId(GradoopId graphId) {
    if (graphIds == null) {
      graphIds = new GradoopIdSet();
    }
    graphIds.add(graphId);
  }

  @Override
  public void setGraphIds(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public void resetGraphIds() {
    if (graphIds != null) {
      graphIds.clear();
    }
  }

  @Override
  public int getGraphCount() {
    return (graphIds != null) ? graphIds.size() : 0;
  }

  @Override
  public String toString() {
    return String.format("%s @ %s", super.toString(), graphIds);
  }
}
