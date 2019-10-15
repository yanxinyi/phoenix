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

import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;

public class PhckTable {
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
    PhckUtil.PHCK_STATE phckState;

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

    public int getParentTableHeadRowColumnCount() {
        if (parent == null) {
            return -1;
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
        return this.headRowColumnCount == this.columnCounter;
    }

    public PhckUtil.PHCK_STATE getPhckState() {
        return this.phckState;
    }

    public void setPhckState(PhckUtil.PHCK_STATE phckState) {
        this.phckState = phckState;
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

    public void addLinkRow(PhckRow row) {
        // todo
    }
}
