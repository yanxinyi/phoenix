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

import java.sql.ResultSet;

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
        this.tenantId = resultSet.getString(1);
        this.tableSchema = resultSet.getString(2);
        this.tableName = resultSet.getString(3);
        this.tableType = resultSet.getString(4);
        this.columnFamily = resultSet.getString(5);
        this.columnName = resultSet.getString(6);
        this.columnCount = resultSet.getString(7);
        this.linkType = resultSet.getString(8);
        this.indexState = resultSet.getString(9);
        this.indexType = resultSet.getString(10);
        this.viewType = resultSet.getString(11);
        this.qualifierCounter = resultSet.getString(12);
        this.phckState = PhckUtil.PHCK_STATE.VALID;
        this.phckRowResource = phckRowResource;
    }

    public String getFullName() {
        return this.tenantId + "," + this.tableSchema + "," + this.tableName;
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

    public String getLinkType() {
        return linkType;
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
