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
package com.facebook.presto.util.querySource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SourceTable
{
    private final String catalogId;
    private final String schema;
    private final String table;
    private final List<Column> columns;

    public SourceTable(
            String catalogId,
            String schema,
            String table,
            List<Column> columns
    )
    {
        this.catalogId = checkNotNull(catalogId, "catalogId is null!");
        this.schema = checkNotNull(schema, "schema is null!");
        this.table = checkNotNull(table, "table is null!");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null!"));
    }

    @JsonProperty("catalogId")
    public String getCatalogId()
    {
        return catalogId;
    }

    @JsonProperty("schema")
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty("table")
    public String getTable()
    {
        return table;
    }

    @JsonProperty("columns")
    public List<Column> getColumns()
    {
        return columns;
    }
}
