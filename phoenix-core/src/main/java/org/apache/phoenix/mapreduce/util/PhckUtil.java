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
import org.apache.phoenix.schema.PTableType;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;

public class PhckUtil {
    public static final String BASE_SELECT_QUERY = "SELECT " +
            TENANT_ID + ", " + TABLE_SCHEM + "," + TABLE_NAME + "," + TABLE_TYPE + "," +
            COLUMN_FAMILY + "," + COLUMN_NAME + "," + COLUMN_COUNT + "," + TABLE_TYPE + "," +
            INDEX_STATE + "," + INDEX_TYPE + "," + VIEW_TYPE + "," + COLUMN_QUALIFIER_COUNTER +
            " FROM " + SYSTEM_CATALOG_NAME;

    public class Table {
        String tenantId;
        String tableSchema;
        String tableName;
        PTableType tableType;
        PIndexState indexState;
        // COLUMN_COUNT FROM SYSTEM.CATALOG VALUE
        int headRowColumnCount;
        // Number of columns belongs to a table
        int columnCounter;
        Table parent;
        Table physicalTable;
        List<Table> children;

        public Table(String tenantId, String tableSchema, String tableName, PTableType tableType,
                     int headRowColumnCount) {
            this.tenantId = tenantId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.tableType = tableType;
            this.headRowColumnCount = headRowColumnCount;
            this.columnCounter = 0;
        }

        public int getParentTableHeadRowColumnCount() {
            if (parent == null) {
                return -1;
            }

            return parent.getHeadRowColumnCount();
        }

        public int getHeadRowColumnCount() {
            return this.headRowColumnCount;
        }

        public void incrementColumnCount() {
            columnCounter++;
        }

        public void addParentTable(Table parent) {
            this.parent = parent;
        }


        public void addPhysicalTable(Table physicalTable) {
            this.physicalTable = physicalTable;
        }

        public void addChildTable(Table child) {
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
    }

    public class Row {
        String tenantId;
        String tableSchema;
        String tableName;
        String tableType;
        String columnFamily;
        String columnName;
        String columnCount;
        String linkType;
        String indexState;
        String indexType;
        String viewType;
        String qualifierCounter;

        Row(ResultSet resultSet) throws Exception {
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
    }
}
