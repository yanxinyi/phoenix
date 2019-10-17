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

import org.apache.phoenix.schema.PTable;

import java.sql.ResultSet;
import java.util.Objects;

import static org.apache.phoenix.mapreduce.util.PhckUtil.constructTableName;
import static org.apache.phoenix.mapreduce.util.PhckUtil.EMPTY_COLUMN_VALUE;
import static org.apache.phoenix.mapreduce.util.PhckUtil.NULL_VALUE;

public class PhckRow {

    private String tenantId;
    private String tableSchema;
    private String tableName;
    private String tableType;
    private String columnFamily;
    private String columnName;
    private String columnCount;
    private String linkType;
    private String indexState;
    private String indexType;
    private String viewType;
    private String qualifierCounter;
    private PhckUtil.PHCK_STATE phckState;
    private PhckUtil.PHCK_ROW_RESOURCE phckRowResource;

    public PhckRow(ResultSet resultSet, PhckUtil.PHCK_ROW_RESOURCE phckRowResource)
            throws Exception {
        this.phckRowResource = phckRowResource;
        this.tenantId = resultSet.getString(1);
        this.tableSchema = resultSet.getString(2);
        this.tableName = resultSet.getString(3);
        this.columnName = resultSet.getString(4);
        this.columnFamily = resultSet.getString(5);
        if (this.columnFamily != null && this.columnFamily.contains(":")) {
            this.columnFamily = this.columnFamily.replace(':', '.');
        }
        this.linkType = resultSet.getString(6);

        if (this.phckRowResource == PhckUtil.PHCK_ROW_RESOURCE.CATALOG) {
            this.columnCount = resultSet.getString(7);
            this.tableType = resultSet.getString(8);
            this.indexState = resultSet.getString(9);
            this.indexType = resultSet.getString(10);
            this.viewType = resultSet.getString(11);
            this.qualifierCounter = resultSet.getString(12);
            this.phckState = PhckUtil.PHCK_STATE.VALID;
        } else {

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
        return this.tenantId + "," + this.tableSchema + "." + this.tableName + "." +
                this.getLinkType() + "." + this.phckRowResource + "." + this.columnFamily + "." +
                this.columnName + "." ;
    }


    public String getFullName() {
        return constructTableName(this.tableName, this.tableSchema, this.tenantId);
    }

    public String getRelatedTableFullName() {
        if (this.columnFamily.contains(".")) {
            return constructTableName(this.columnFamily, null, this.tenantId);
        }
        return constructTableName(this.columnFamily, this.tableSchema, this.tenantId);

    }

    public boolean isSchemaRow() {
        return (this.tableName == null || this.tableName.equals(EMPTY_COLUMN_VALUE))
                && this.tenantId == null && this.tableSchema != null;
    }

    public boolean isHeadRow() {
        return this.tableName != null && this.columnCount != null && this.tableType != null;
    }

    public boolean isColumnRow() {
        return this.tableName != null && this.columnName != null;
    }

    public boolean isQualifierCounterRow() {
        return this.tableName != null && this.qualifierCounter != null;
    }

    public boolean isLinkRow() {
        return this.linkType != null && this.columnFamily != null;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getTableSchema() {
        return this.tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnCount() {
        return columnCount;
    }

    public PTable.LinkType getLinkType() {
        if (linkType == null) {
            return null;
        }
        return PTable.LinkType.fromSerializedValue(Byte.valueOf(linkType));
    }

    public String getIndexState() {
        return indexState;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getViewType() {
        return viewType;
    }

    public String getQualifierCounter() {
        return qualifierCounter;
    }

    public PhckUtil.PHCK_STATE getPhckState() {
        return this.phckState;
    }

    public void setPhckState(PhckUtil.PHCK_STATE phckState) {
        this.phckState = phckState;
    }

    public PhckUtil.PHCK_ROW_RESOURCE getPhckRowResource() {
        return phckRowResource;
    }
}
