package org.apache.phoenix.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.util.PhckUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

public class PhoenixIntegrityCheckAndRepairTool extends Configured implements Tool {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIntegrityCheckAndRepairTool.class);

    private HashSet<PhckUtil.Row> orphanRowSet = new HashSet<>();
    /**
     SELECT
        TENANT_ID, TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,
        COLUMN_FAMILY,COLUMN_NAME,COLUMN_COUNT,LINK_TYPE,
        INDEX_STATE,INDEX_TYPE,VIEW_TYPE,QUALIFIER_COUNTER
     FROM SYSTEM.CATALOG
        WHERE TABLE_SCHEM NOT LIKE 'SYSTEM%';
     */
    private static final String selectQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_NAME + "," +
            COLUMN_COUNT + "," +
            TABLE_TYPE + "," +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() +"' AND NOT " +
            VIEW_TYPE + " = " + PTable.ViewType.MAPPED.getSerializedValue();



    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PhoenixIntegrityCheckAndRepairTool(), args);
        System.exit(result);
//        Configuration conf = HBaseConfiguration.create();
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        admin.getTableDescriptor("table");
    }
}
