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
package org.gradoop.flink.io.impl.csv.indexed.functions;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.tuples.CSVElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is an OutputFormat to serialize {@link Tuple}s to text by their labels.
 * The output is structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 *
 * @param <T> Tuple that will be written to csv
 *
 * references to: org.apache.flink.api.java.io.CSVOutputFormat
 */
public class IndexedCSVFileFormat<T extends Tuple & CSVElement> extends
  MultipleFileOutputFormat<T> {

  /**
   * The default line delimiter if no one is set.
   */
  public static final String DEFAULT_LINE_DELIMITER = CSVConstants.ROW_DELIMITER;

  /**
   * The default field delimiter if no is set.
   */
  public static final String DEFAULT_FIELD_DELIMITER = CSVConstants.TOKEN_DELIMITER;

  // --------------------------------------------------------------------------------

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(IndexedCSVFileFormat.class);

  // --------------------------------------------------------------------------------

  /**
   * The character the fields should be separated.
   */
  private String fieldDelimiter;

  /**
   * The character the lines should be separated.
   */
  private String recordDelimiter;

  /**
   * The charset that is used for the output encoding.
   */
  private String charsetName = null;

  /**
   * Creates a new instance of an IndexedCSVFileFormat. Use the default record delimiter '\n'
   * and the default field delimiter ','.
   *
   * @param outputPath The path where the CSV file will be written.
   */
  public IndexedCSVFileFormat(Path outputPath) {
    this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
  }

  /**
   * Creates a new instance of an IndexedCSVFileFormat. Use the default record delimiter '\n'.
   *
   * @param outputPath The path where the CSV file will be written.
   * @param fieldDelimiter The field delimiter for the CSV file.
   */
  public IndexedCSVFileFormat(Path outputPath, String fieldDelimiter) {
    this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
  }

  /**
   * Creates a new instance of an IndexedCSVFileFormat.
   *
   * @param outputPath The path where the CSV file will be written.
   * @param recordDelimiter The record delimiter for the CSV file.
   * @param fieldDelimiter The field delimiter for the CSV file.
   */
  public IndexedCSVFileFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
    super(outputPath);
    if (recordDelimiter == null) {
      throw new IllegalArgumentException("RecordDelmiter shall not be null.");
    }

    if (fieldDelimiter == null) {
      throw new IllegalArgumentException("FieldDelimiter shall not be null.");
    }

    this.fieldDelimiter = fieldDelimiter;
    this.recordDelimiter = recordDelimiter;
  }

  /**
   * Sets the charset with which the CSV strings are written to the file.
   * If not specified, the output format uses the systems default character encoding.
   *
   * @param charsetName The name of charset to use for encoding the output.
   */
  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }


  @Override
  protected OutputFormat<T> createFormatForDirectory(Path directory) throws IOException {
    CsvOutputFormat<T> format = new CsvOutputFormat<>(directory, recordDelimiter, fieldDelimiter);
    format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);
    format.initializeGlobal(parallelism);
    format.configure(configuration);
    format.open(taskNumber, numTasks);
    if (charsetName != null) {
      format.setCharsetName(charsetName);
    }
    return format;
  }

  @Override
  protected String getDirectoryForRecord(T record) {
    String label = record.getLabel();
    if (label.isEmpty()) {
      return cleanFilename(CSVConstants.DEFAULT_DIRECTORY) + Path.SEPARATOR +
        CSVConstants.SIMPLE_FILE;
    } else {
      return cleanFilename(label) + Path.SEPARATOR + CSVConstants.SIMPLE_FILE;
    }
  }
}

