package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.util.SchemaUtil;

public class IndexNotFoundException extends MetaDataEntityNotFoundException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.INDEX_UNDEFINED;
    private final long timestamp;

    public IndexNotFoundException(IndexNotFoundException e, long timestamp) {
        this(e.getSchemaName(),e.getTableName(), timestamp);
    }

    public IndexNotFoundException(String tableName) {
        this(SchemaUtil.getSchemaNameFromFullName(tableName), SchemaUtil.getTableNameFromFullName(tableName));
    }

    public IndexNotFoundException(String schemaName, String tableName) {
        this(schemaName, tableName, HConstants.LATEST_TIMESTAMP);
    }

    public IndexNotFoundException(String schemaName, String tableName, long timestamp) {
        super(new SQLExceptionInfo.Builder(code).setSchemaName(schemaName).setTableName(tableName).build().toString(),
                code.getSQLState(), code.getErrorCode(), schemaName, tableName, null);
        this.timestamp = timestamp;
    }

    public long getTimeStamp() {
        return timestamp;
    }
}