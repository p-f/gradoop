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
package org.gradoop.flink.model.impl.operators.grouping;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.LabelGroupFilter;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;

/**
 * Grouping implementation that uses group + groupReduce for building super
 * vertices and updating the original vertices.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
 * 2) Group vertices on label and/or property.
 * 3) Create a super vertex id for each group and collect a non-candidate
 *    {@link VertexGroupItem} for each group element and one additional
 *    super vertex tuple that holds the group aggregate.
 * 4) Filter output of 3)
 *    a) non-candidate tuples are mapped to {@link VertexWithSuperVertex}
 *    b) super vertex tuples are used to build final super vertices
 * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
 * 6) Join edges with output of 4a) and replace source/target id with super
 *    vertex id.
 * 7) Updated edges are grouped by source and target id and optionally by label
 *    and/or edge property.
 * 8) Group combine on the workers and compute aggregate.
 * 9) Group reduce globally and create final super edges.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class GroupingGroupReduce<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends Grouping<G, V, E, LG, GC> {
  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels             group on vertex label true/false
   * @param useEdgeLabels               group on edge label true/false
   * @param vertexLabelGroups           stores grouping properties for vertex labels
   * @param edgeLabelGroups             stores grouping properties for edge labels
   * @param retainVerticesWithoutGroup  a flag to retain vertices that are not affected by the
   *                                    grouping
   */
  GroupingGroupReduce(boolean useVertexLabels, boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups, List<LabelGroup> edgeLabelGroups,
    boolean retainVerticesWithoutGroup) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups,
      retainVerticesWithoutGroup);
  }

  @Override
  protected LG groupInternal(LG graph) {

    DataSet<V> vertices = isRetainingVerticesWithoutGroup() ?
      graph.getVertices()
        .filter(new LabelGroupFilter<>(getVertexLabelGroups(), useVertexLabels())) :
      graph.getVertices();

    // map vertex to vertex group item
    DataSet<VertexGroupItem> verticesForGrouping = vertices.flatMap(
      new BuildVertexGroupItem<>(useVertexLabels(), getVertexLabelGroups()));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> vertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceVertexGroupItems(useVertexLabels()));

    DataSet<V> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterSuperVertices())
      // build super vertices
      .map(new BuildSuperVertex<>(useVertexLabels(), graph.getFactory().getVertexFactory()));

    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = vertexGroupItems
      // filter group element tuples
      .filter(new FilterRegularVertices())
      // build vertex to group representative tuple
      .map(new BuildVertexWithSuperVertex());

    DataSet<E> edgesToGroup = graph.getEdges();

    if (isRetainingVerticesWithoutGroup()) {
      LG retainedVerticesSubgraph = getSubgraphOfRetainedVertices(graph);

      // To add support for grouped edges between retained vertices and supervertices,
      // vertices are their group representatives themselves
      vertexToRepresentativeMap =
        updateVertexRepresentatives(vertexToRepresentativeMap,
          retainedVerticesSubgraph.getVertices());

      // don't execute grouping on edges between retained vertices
      // but execute on edges between retained vertices and grouped vertices
      //   graph.getEdges() - retainedVerticesSubgraph.getEdges()
      edgesToGroup = subtractEdges(graph.getEdges(), retainedVerticesSubgraph.getEdges());
    }

    DataSet<E> superEdges =
      buildSuperEdges(graph.getFactory().getEdgeFactory(), edgesToGroup, vertexToRepresentativeMap);

    if (isRetainingVerticesWithoutGroup()) {
      LG retainedVerticesSubgraph = getSubgraphOfRetainedVertices(graph);
      superVertices = superVertices.union(retainedVerticesSubgraph.getVertices());
      superEdges = superEdges.union(retainedVerticesSubgraph.getEdges());
    }

    return graph.getFactory().fromDataSets(superVertices, superEdges);
  }

}
