package org.apache.phoenix.end2end;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhckSystemTableValidator;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(NeedsOwnMiniClusterTest.class)
public class PHCKSystemTableValidatorIT extends ParallelStatsEnabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PHCKSystemTableValidatorIT.class);

    @Test
    public void testDisabledTable() throws Exception {

        String SELECT_QUERY = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + "." + SYSTEM_STATS_TABLE;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection connection = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        Admin admin = phoenixConnection.getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." + SYSTEM_STATS_TABLE));
        // Running select query on System.Function table to verify if its disabled.
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        runPhckSystemTableValidator(true,true,null);
        ResultSet viewRS1 = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        assertNotNull(viewRS1);
    }

    @Test
    public void testDroppedTable() throws Exception {
        String SELECT_QUERY = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + "." + SYSTEM_STATS_TABLE;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection connection = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        Admin admin = phoenixConnection.getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." +SYSTEM_STATS_TABLE));
        admin.deleteTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." +SYSTEM_STATS_TABLE));
        try{
            ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        } catch ( TableNotFoundException e) {
            LOGGER.warn("Table was deleted by hbase");
        }
        runPhckSystemTableValidator(true,true,null);
        ResultSet viewRS1 = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        assertNotNull(viewRS1);
    }
    private static String[] getArgValues(boolean invalidCount, boolean outputPath) {
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add("/tmp");
        }
        if (invalidCount) {
            args.add("-c");
        }

        return args.toArray(new String[0]);
    }

    public static int runPhckSystemTableValidator(boolean getCorruptedViewCount, boolean outputPath, PhckSystemTableValidator tool)
            throws Exception {
        if (tool == null) {
            tool = new PhckSystemTableValidator();
        }
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}