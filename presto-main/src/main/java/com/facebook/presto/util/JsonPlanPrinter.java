/*
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
package com.facebook.presto.util;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.util.querySource.Column;
import com.facebook.presto.util.querySource.SourceTable;
import com.facebook.presto.util.querySource.SourceTables;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonPlanPrinter
{
    private ImmutableList.Builder<SourceTable> sourceTableBuilder = ImmutableList.builder();

    public JsonPlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(types, "types is null");
        checkNotNull(metadata, "metadata is null");
        SourceVisitor visitor = new SourceVisitor(types, metadata);
        plan.accept(visitor, null);
    }

    public String printSource()
    {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            SourceTables sourceTables = new SourceTables(sourceTableBuilder.build());
            return objectMapper.writeValueAsString(sourceTables);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    private class SourceVisitor
            extends PlanVisitor<Void, Void>
    {
        private final Map<Symbol, Type> types;
        private final Metadata metadata;

        public SourceVisitor(Map<Symbol, Type> types, Metadata metadata)
        {
            this.types = types;
            this.metadata = metadata;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getLeft().accept(this, null);
            node.getRight().accept(this, null);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            node.getSource().accept(this, null);
            node.getFilteringSource().accept(this, null);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitWindow(final WindowNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TupleDomain partitionsDomainSummary = node.getPartitionsDomainSummary();
            TableMetadata tableMetadata = metadata.getTableMetadata(node.getTable());

            ImmutableList.Builder<Column> columnBuilder = ImmutableList.builder();


            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                boolean isOutputSymbol = node.getOutputSymbols().contains(entry.getKey());
                boolean isInOriginalConstraint = DependencyExtractor.extractUnique(node.getOriginalConstraint()).contains(entry.getKey());
                boolean isInDomainSummary = !partitionsDomainSummary.isNone() && partitionsDomainSummary.getDomains().keySet().contains(entry.getValue());

                if (isOutputSymbol || isInOriginalConstraint || isInDomainSummary) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(node.getTable(), entry.getValue());
                    Domain domain = null;
                    if (isInDomainSummary) {
                        domain = partitionsDomainSummary.getDomains().get(entry.getValue());
                    }
                    else if (partitionsDomainSummary.isNone()) {
                        domain = Domain.none(columnMetadata.getType().getNativeType());
                    }
                    Column column = new Column(columnMetadata.getName(), columnMetadata.getType().toString(), domain);
                    columnBuilder.add(column);
                }
            }
            SourceTable sourceTable = new SourceTable(
                    tableMetadata.getConnectorId(),
                    tableMetadata.getTable().getSchemaName(),
                    tableMetadata.getTable().getTableName(),
                    columnBuilder.build()
            );
            sourceTableBuilder.add(sourceTable);
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitTopN(final TopNNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitSort(final SortNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitMaterializedViewWriter(MaterializedViewWriterNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            return null;
        }

        @Override
        public Void visitSink(SinkNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        public Void visitTableCommit(TableCommitNode node, Void context)
        {
            return processChildren(node);
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, null);
            }

            return null;
        }
    }
}
