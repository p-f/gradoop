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
package org.gradoop.flink.model.impl.operators.tpgm.diff;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.tpgm.diff.functions.DiffPerElement;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

import java.util.Objects;

/**
 * Calculates the difference between two snapshots of a graph and stores the result in a property.
 * The result will be a number indicating that an element is either equal in both snapshots or
 * added/removed in the second snapshot. Elements not present in both snapshots will be discarded.
 */
public class Diff implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {
  /**
   * The property key used to store the diff result on each element.
   */
  public static final String PROPERTY_KEY = "_diff";

  /**
   * The property value used to indicate that an element was added in the second snapshot.
   */
  public static final PropertyValue VALUE_ADDED = PropertyValue.create(1);

  /**
   * The property value used to indicate that an element is equal in both snapshots.
   */
  public static final PropertyValue VALUE_EQUAL = PropertyValue.create(0);

  /**
   * The property value used to indicate that an element was removed in the second snapshot.
   */
  public static final PropertyValue VALUE_REMOVED = PropertyValue.create(-1);

  /**
   * The predicate used to determine the first snapshot.
   */
  private final TemporalPredicate first;

  /**
   * The predicate used to determine the second snapshot.
   */
  private final TemporalPredicate second;

  /**
   * Should the edge set be validated after this operator?
   */
  private final boolean validate;

  /**
   * Create an instance of the TPGM diff operator, setting the two predicates used to determine
   * the snapshots.
   *
   * @param firstPredicate  The predicate used for the first snapshot.
   * @param secondPredicate The predicate used for the second snapshot.
   * @param validate        Should the graph be validated?
   */
  public Diff(TemporalPredicate firstPredicate, TemporalPredicate secondPredicate,
    boolean validate) {
    this.first = Objects.requireNonNull(firstPredicate);
    this.second = Objects.requireNonNull(secondPredicate);
    this.validate = validate;
  }

  /**
   * Create an instance of the TPGM diff operator, setting the two predicates used to determine
   * the snapshots.
   * This will not validate the graph.
   *
   * @param firstPredicate  The predicate used for the first snapshot.
   * @param secondPredicate The predicate used for the second snapshot.
   */
  public Diff(TemporalPredicate firstPredicate, TemporalPredicate secondPredicate) {
    this(firstPredicate, secondPredicate, false);
  }

  @Override
  public TemporalGraph execute(TemporalGraph graph) {
    DataSet<TemporalVertex> transformedVertices = graph.getVertices()
      .flatMap(new DiffPerElement<>(first, second));
    DataSet<TemporalEdge> transformedEdges = graph.getEdges()
      .flatMap(new DiffPerElement<>(first, second));
    if (validate) {
      transformedEdges = transformedEdges.join(transformedVertices)
        .where(new SourceId<>()).equalTo(new Id<>()).with(new LeftSide<>())
        .join(transformedVertices)
        .where(new TargetId<>()).equalTo(new Id<>()).with(new LeftSide<>());
    }
    return graph.getFactory().fromDataSets(transformedVertices, transformedEdges);
  }
}
