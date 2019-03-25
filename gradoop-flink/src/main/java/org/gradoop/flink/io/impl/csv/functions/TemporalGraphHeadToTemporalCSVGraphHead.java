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
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.flink.io.api.metadata.MetaDataSource;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.TemporalCSVGraphHead;

/**
 * Converts an {@link TemporalGraphHead} into a CSV representation.
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class TemporalGraphHeadToTemporalCSVGraphHead
  extends ElementToCSV<TemporalGraphHead, TemporalCSVGraphHead> {

  /**
   * Reduce object instantiations.
   */
  private final TemporalCSVGraphHead csvGraphHead = new TemporalCSVGraphHead();

  @Override
  public TemporalCSVGraphHead map(TemporalGraphHead temporalGraphHead) throws Exception {
    csvGraphHead.setId(temporalGraphHead.getId().toString());
    csvGraphHead.setLabel(StringEscaper.escape(temporalGraphHead.getLabel(),
      CSVConstants.ESCAPED_CHARACTERS));
    csvGraphHead.setProperties(getPropertyString(temporalGraphHead, MetaDataSource.VERTEX_TYPE));
    csvGraphHead.setTemporalData(getTemporalDataString(temporalGraphHead.getTransactionTime(),
      temporalGraphHead.getValidTime()));
    return csvGraphHead;
  }
}
