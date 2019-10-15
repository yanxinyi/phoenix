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

    public PhckRow(ResultSet resultSet) throws Exception {
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
