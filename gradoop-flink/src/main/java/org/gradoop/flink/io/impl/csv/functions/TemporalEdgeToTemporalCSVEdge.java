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
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.TemporalCSVEdge;

/**
 * Converts an {@link TemporalEdge} into a CSV representation.
 */
@FunctionAnnotation.ForwardedFields("label->f4")
public class TemporalEdgeToTemporalCSVEdge extends ElementToCSV<TemporalEdge, TemporalCSVEdge> {

  /**
   * Reduce object instantiations.
   */
  private final TemporalCSVEdge csvEdge = new TemporalCSVEdge();

  @Override
  public TemporalCSVEdge map(TemporalEdge temporalEdge) throws Exception {
    csvEdge.setId(temporalEdge.getId().toString());
    csvEdge.setGradoopIds(collectionToCsvString(temporalEdge.getGraphIds()));
    csvEdge.setSourceId(temporalEdge.getSourceId().toString());
    csvEdge.setTargetId(temporalEdge.getTargetId().toString());
    csvEdge.setLabel(StringEscaper.escape(temporalEdge.getLabel(),
      CSVConstants.ESCAPED_CHARACTERS));
    csvEdge.setProperties(getPropertyString(temporalEdge, MetaDataSource.EDGE_TYPE));
    csvEdge.setTemporalData(getTemporalDataString(temporalEdge.getTransactionTime(),
      temporalEdge.getValidTime()));
    return csvEdge;
  }
}
