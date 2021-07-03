/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce.util;

import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.hadoop.hbase.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.mapreduce.util.PhckUtil.EMPTY_COLUMN_VALUE;
import static org.apache.phoenix.mapreduce.util.PhckUtil.NULL_VALUE;
import static org.apache.phoenix.mapreduce.util.PhckUtil.constructTableName;

/**
 *
 * +-----------+-------------+--------------+------------+---------------+-------------+--------------+-----------+-------------+------------+-----------+-------------------+
 * | TENANT_ID | TABLE_SCHEM |  TABLE_NAME  | TABLE_TYPE | COLUMN_FAMILY | COLUMN_NAME | COLUMN_COUNT | LINK_TYPE | INDEX_STATE | INDEX_TYPE | VIEW_TYPE | QUALIFIER_COUNTER |
 * +-----------+-------------+--------------+------------+---------------+-------------+--------------+-----------+-------------+------------+-----------+-------------------+
 * |           |             | AAA          | u          |               |             | 3            | null      |             | null       | null      | null              |
 * |           |             | AAA          |            | 0             |             | null         | null      |             | null       | null      | 14                |
 * |           |             | AAA          | i          | AAA_INDEX     |             | null         | 1         |             | null       | null      | null              |
 * |           |             | AAA          |            |               | A           | null         | null      |             | null       | null      | null              |
 * |           |             | AAA          |            | 0             | B           | null         | null      |             | null       | null      | null              |
 * |           |             | AAA          |            | 0             | C           | null         | null      |             | null       | null      | null              |
 * |           |             | AAA_INDEX    | i          |               |             | 3            | null      | a           | 1          | null      | null              |
 * |           |             | AAA_INDEX    |            | 0             |             | null         | null      |             | null       | null      | 13                |
 * |           |             | AAA_INDEX    |            |               | 0:B         | null         | null      |             | null       | null      | null              |
 * |           |             | AAA_INDEX    |            | 0             | 0:C         | null         | null      |             | null       | null      | null              |
 * |           |             | AAA_INDEX    |            |               | :A          | null         | null      |             | null       | null      | null              |
 * |           |             | AAA_INDEX_V1 | v          |               |             | 4            | null      |             | null       | 3         | null              |
 * |           |             | AAA_INDEX_V1 |            | AAA_INDEX     |             | null         | 2         |             | null       | null      | null              |
 * |           |             | AAA_INDEX_V1 |            | 0             | E           | null         | null      |             | null       | null      | null              |
 * |           |             | AAA_INDEX_V2 | v          |               |             | 3            | null      |             | null       | 3         | null              |
 * |           |             | AAA_INDEX_V2 |            | AAA_INDEX     |             | null         | 2         |             | null       | null      | null              |
 * |           |             | AAA_V1       | v          |               |             | 3            | null      |             | null       | 3         | null              |
 * |           |             | AAA_V1       |            | AAA           |             | null         | 2         |             | null       | null      | null              |
 * |           |             | AAA_V2       | v          |               |             | 4            | null      |             | null       | 2         | null              |
 * |           |             | AAA_V2       |            | AAA           |             | null         | 2         |             | null       | null      | null              |
 * |           |             | AAA_V2       |            | AAA_V1        |             | null         | 3         |             | null       | null      | null              |
 * |           |             | AAA_V2       |            | 0             | D           | null         | null      |             | null       | null      | null              |
 * |           |             | FOO          | u          |               |             | 2            | null      |             | null       | null      | null              |
 * |           |             | FOO          |            | 0             |             | null         | null      |             | null       | null      | 12                |
 * |           |             | FOO          |            |               | A           | null         | null      |             | null       | null      | null              |
 * |           |             | FOO          |            | 0             | B           | null         | null      |             | null       | null      | null              |
 * +-----------+-------------+--------------+------------+---------------+-------------+--------------+-----------+-------------+------------+-----------+-------------------+
 */
public class PhckTable {
    private final int INVALID_PARENT_COLUMN_COUNT = -1;
    String tenantId;
    String tableSchema;
    String tableName;
    PTableType tableType;
    PIndexState indexState;
    // COLUMN_COUNT FROM SYSTEM.CATALOG VALUE
    int headRowColumnCount;
    // Number of columns belongs to a table
    int columnCounter;
    int numOfQualifierCountRow;
    PhckTable parent;
    PhckTable physicalTable;
    List<PhckTable> children;
    private List<PhckRow> relationRows;
    PhckUtil.PHCK_STATE phckState;
    private TableName indexPhysicalName;


    public PhckTable(String tenantId, String tableSchema, String tableName, String tableType,
                     String headRowColumnCount, String indexState) {
        this.tenantId = tenantId;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.headRowColumnCount = Integer.valueOf(headRowColumnCount);
        this.columnCounter = 0;
        this.numOfQualifierCountRow = 0;
        this.phckState = PhckUtil.PHCK_STATE.VALID;

        if (tableType == null) {
            this.tableType = null;
        } else {
            this.tableType = PTableType.fromSerializedValue(tableType);
        }
        if (indexState == null) {
            this.indexState = null;
        } else {
            this.indexState = PIndexState.fromSerializedValue(indexState);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSerializedValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (getClass() != obj.getClass())
            return false;
        PhckTable other = (PhckTable) obj;
        if (this.getSerializedValue().compareTo(other.getSerializedValue()) != 0)
            return false;
        return true;
    }

    public String getSerializedValue() {
        return this.tenantId + "," + this.tableSchema + "."
                + this.tableName + "." + this.tableType;
    }

    public TableName getHBaseTableName() {
        if (this.tableSchema == null || this.tableSchema.equals(NULL_VALUE)
                || this.tableSchema.equals(EMPTY_COLUMN_VALUE)) {
            return TableName.valueOf(this.tableName);
        }
        return TableName.valueOf(this.tableSchema + ":" + this.tableName);
    }

    public String getFullName() {
        return constructTableName(this.tableName, this.tableSchema, this.tenantId);
    }

    public int getParentTableHeadRowColumnCount() {
        if (parent == null) {
            return INVALID_PARENT_COLUMN_COUNT;
        }

        return parent.getHeadRowColumnCount();
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public PTableType getTableType() {
        return tableType;
    }

    public PIndexState getIndexState() {
        return indexState;
    }

    public int getColumnCounter() {
        return columnCounter;
    }

    public PhckTable getParent() {
        return parent;
    }

    public PhckTable getPhysicalTable() {
        return physicalTable;
    }

    public List<PhckTable> getChildren() {
        return children;
    }

    public int getHeadRowColumnCount() {
        return this.headRowColumnCount;
    }

    public void incrementColumnCount() {
        this.columnCounter++;
    }

    public void incrementQualifierCount() {
        this.numOfQualifierCountRow++;
    }

    public void addParentTable(PhckTable parent) {
        this.parent = parent;
    }


    public void addPhysicalTable(PhckTable physicalTable) {
        this.physicalTable = physicalTable;
    }

    public void addChildTable(PhckTable child) {
        if (children == null) {
            children = new ArrayList<>();
        }

        children.add(child);
    }

    public boolean isIndexDisabled() {
        if (this.indexState == PIndexState.DISABLE ||
                this.indexState == PIndexState.INACTIVE ||
                this.indexState == PIndexState.UNUSABLE) {
            return true;
        }
        return false;
    }

    public boolean isSystemTable() {
        return this.tableSchema != null && this.tableSchema.equals(SYSTEM_SCHEMA_NAME) &&
                this.tableName != null &&
                this.tableType != null && this.tableType == PTableType.SYSTEM;
    }

    public boolean isIndexTable() {
        return this.tableName != null &&
                this. tableType != null && this.tableType == PTableType.INDEX;
    }

    public boolean isPhysicalTable() {
        return this.tableName != null &&
                this.tableType != null && this.tableType == PTableType.TABLE;
    }

    public boolean isViewTable() {
        return this.tableName != null &&
                this.tableType != null && this.tableType == PTableType.VIEW;
    }

    public boolean isColumnCountMatches() {
        if (isViewTable()) {
            return this.getParentTableHeadRowColumnCount() != INVALID_PARENT_COLUMN_COUNT &&
                    this.headRowColumnCount ==
                            this.columnCounter + this.getParentTableHeadRowColumnCount();
        }
        return this.headRowColumnCount == this.columnCounter;
    }

    public PhckUtil.PHCK_STATE getPhckState() {
        return this.phckState;
    }

    public void setPhckState(PhckUtil.PHCK_STATE phckState) {
        this.phckState = phckState;
    }

    public void setParent(PhckTable parent) {
        this.parent = parent;
    }

    public void setPhysicalTable(PhckTable physicalTable) {
        this.physicalTable = physicalTable;
    }

    public void setIndexPhysicalName(String indexPhysicalName) {
        if (indexPhysicalName.contains(".")) {
            indexPhysicalName = indexPhysicalName.replace('.', ':');
        }
        this.indexPhysicalName = TableName.valueOf(indexPhysicalName);
    }

    public TableName getIndexPhysicalName() {
        return indexPhysicalName;
    }

    public boolean isValidQualifierRowCount() {
        if (this.tableType == PTableType.VIEW && this.numOfQualifierCountRow != 0) {
            return false;
        } else if (this.tableType == PTableType.TABLE && this.numOfQualifierCountRow != 1) {
            return false;
        } else if (this.tableType == PTableType.INDEX && this.numOfQualifierCountRow != 1) {
            return false;
        }

        return true;
    }

    public boolean hasPendingRelationRowToProcess() {
        return this.relationRows != null;
    }
    public List<PhckRow> getRelationRows() {
        return this.relationRows;
    }

    public void addLinkRow(PhckRow row) {
        if (this.relationRows == null) {
            this.relationRows = new ArrayList<>();
        }
        this.relationRows.add(row);
    }
}
