package org.apache.phoenix.mapreduce.util;

import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;

import java.sql.ResultSet;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;

public class PhckUtil {
    public class Table {
        String tenantId;
        String schema;
        String tableName;
        PTableType tableType;
        PIndexState indexState;
        // COLUMN_COUNT FROM SYSTEM.CATALOG VALUE
        int headRowColumnCount;
        // Number of columns belongs to a table
        int columnCounter;
        Table parent;
        Table root;
        List<Table> children;

        public boolean isIndexDisabled() {
            if (this.indexState == PIndexState.DISABLE ||
                    this.indexState == PIndexState.INACTIVE ||
                    this.indexState == PIndexState.UNUSABLE) {
                return true;
            }
            return false;
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

        public boolean isSystemTable() {
            return this.tableSchema != null && this.tableSchema.equals(SYSTEM_SCHEMA_NAME) &&
                    this.tableName != null &&
                    this.tableType != null && this.tableType.equals(PTableType.SYSTEM);
        }

        public boolean isIndexTable() {
            return this.tableName != null &&
                    this. tableType != null && this.tableType.equals(PTableType.INDEX);
        }

        public boolean isPhysicalTable() {
            return this.tableName != null &&
                    this.tableType != null && this.tableType.equals(PTableType.TABLE);
        }

        public boolean isViewTable() {
            return this.tableName != null &&
                    this.tableType != null && this.tableType.equals(PTableType.VIEW);
        }

        public boolean isLinkRow() {
            return this.linkType != null && this.columnFamily != null;
        }
    }
}
