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
package org.apache.phoenix.mapreduce;

import org.antlr.runtime.CharStream;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SpanReceiver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfo;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.thrift.transport.TTransportException;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.joda.time.Chronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

public class ViewTTLTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLTool.class);

    public static final String RUNNING_FOR_DELETE_ALL_VIEWS_STRING = "RUNNING_FOR_DELETE_ALL_VIEWS";
    public static final String SELECT_ALL_VIEW_FROM_CHILD_LINK_QUERY =
            "SELECT TENANT_ID, TABLE_SCHEM, COLUMN_NAME, COLUMN_FAMILY FROM " +
            SYSTEM_CHILD_LINK_NAME + " WHERE " +
            LINK_TYPE + " = 4 ";
    public static final String SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY =
            "SELECT TENANT_ID, TABLE_SCHEM, COLUMN_COUNT FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private final String GET_ALL_VIEW_TTL_QUERY = "SELECT TENANT_ID, TABLE_NAME, COLUMN_COUNT FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private static final Option TABLE_NAME_OPTION = new Option("t", "table", true,
            "Phoenix Table Name");
    private static final Option JOB_PRIORITY_OPTION = new Option("p", "job-priority", true,
            "Define job priority from 0(highest) to 6");
    private static final Option RUN_FOREGROUND_OPTION =
            new Option("runfg", "run-foreground", false,
                    "If specified, runs UpdateStatisticsTool in Foreground. Default - Runs the build in background");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    Configuration configuration;
    Connection connection;

    private String baseTableName;
    private JobPriority jobPriority;
    private boolean isForeground;
    private Job job;


    void parseArgs(String[] args) {
        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }

        if (getConf() == null) {
            setConf(HBaseConfiguration.create());
        }

        baseTableName = cmdLine.getOptionValue(TABLE_NAME_OPTION.getOpt(),
                RUNNING_FOR_DELETE_ALL_VIEWS_STRING);
        jobPriority = getJobPriority(cmdLine);
        isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
    }

    public String getJobPriority() {
        return this.jobPriority.toString();
    }

    private JobPriority getJobPriority(CommandLine cmdLine) {
        String jobPriorityOption = cmdLine.getOptionValue(JOB_PRIORITY_OPTION.getOpt());
        if (jobPriorityOption == null) {
            return JobPriority.NORMAL;
        }

        switch (jobPriorityOption) {
            case "0" : return JobPriority.VERY_HIGH;
            case "1" : return JobPriority.HIGH;
            case "2" : return JobPriority.NORMAL;
            case "3" : return JobPriority.LOW;
            case "4" : return JobPriority.VERY_LOW;
            default:
                return JobPriority.NORMAL;
        }
    }

    CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

//        if (!cmdLine.hasOption(TABLE_NAME_OPTION.getOpt())) {
//            throw new IllegalStateException(TABLE_NAME_OPTION.getLongOpt() + " is a mandatory "
//                    + "parameter");
//        }
        this.jobPriority = getJobPriority(cmdLine);

        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_NAME_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(JOB_PRIORITY_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);

        return options;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    private void configureJob() throws Exception {

        if (this.baseTableName == null) {
            this.baseTableName = RUNNING_FOR_DELETE_ALL_VIEWS_STRING;
        }
        this.job = Job.getInstance(getConf(),
                "ViewTTLTool-" + this.baseTableName + "-" +  System.currentTimeMillis());
        job.setInputFormatClass(PhoenixMultiViewInputFormat.class);
        Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputTableName(configuration, this.baseTableName);
        PhoenixConfigurationUtil.setSchemaType(configuration, PhoenixConfigurationUtil.SchemaType.QUERY);

        job.setJarByClass(ViewTTLTool.class);
        job.setMapperClass(ViewTTLDeleteJobMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setPriority(this.jobPriority);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(), PhoenixConnection.class, Chronology.class,
                CharStream.class, TransactionSystemClient.class, TransactionNotInProgressException.class,
                ZKClient.class, DiscoveryServiceClient.class, ZKDiscoveryService.class,
                Cancellable.class, TTransportException.class, SpanReceiver.class, TransactionProcessor.class);
        LOGGER.info("ViewTTLTool running for: " + baseTableName);
    }

    private int runJob() {
        try {
            if (isForeground) {
                LOGGER.info("Running ViewTTLTool in Foreground. " +
                        "Runs full table scans. This may take a long time!");
                return (job.waitForCompletion(true)) ? 0 : 1;
            } else {
                LOGGER.info("Running ViewTTLTool in Background - Submit async and exit");
                job.submit();
                return 0;
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception " + e + " trying to run ViewTTLTool.");
            return 1;
        }
    }

    public static class ViewTTLDeleteJobMapper
            extends Mapper<NullWritable, ViewInfo, NullWritable, NullWritable> {

        @Override
        protected void map(NullWritable key, ViewInfo value,
                           Context context) {
            System.out.println("*******");
            System.out.println("Tenant ID: " + value.getTenantId());
            System.out.println("VIEW NAME: " + value.getViewName());
            System.out.println("TTL: " + value.getViewTtl());


            try {
                final Configuration config = context.getConfiguration();
                PhoenixConnection connection = (PhoenixConnection) ConnectionUtil.getInputConnection(config);
                if (value.getTenantId() != null && !value.getTenantId().equals("NULL")) {
                    connection = (PhoenixConnection) ViewTTLTool.buildTenantConnection(connection.getURL(), value.getTenantId());
                }

                /***
                 * later on we will inject scan attribute TTL
                 */
//                final Statement statement = connection.createStatement();
//                final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
                String selectStatement = "SELECT count(*) FROM " + value.getViewName();
                ResultSet rs = connection.createStatement().executeQuery(selectStatement);
                while (rs.next()) {
                    System.out.println("Row count : " + rs.getString(1));
                }

                System.out.println("*******");
            } catch (SQLException e) {
                LOGGER.error(e.getErrorCode() + e.getSQLState(), e.getStackTrace());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        connection = null;
        int ret;
        try {
            parseArgs(args);
            configuration = HBaseConfiguration.addHbaseResources(getConf());
            connection = ConnectionUtil.getInputConnection(configuration, new Properties());
            configureJob();
            TableMapReduceUtil.initCredentials(job);
            ret = runJob();
        } catch (Exception e) {
            printHelpAndExit(e.getMessage(), getOptions());
            return -1;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        return ret;
    }

    public static Connection buildTenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    public static List<ViewInfo> getViewsWithTTL(Configuration configuration) {
        String baseTableName = PhoenixConfigurationUtil.getInputTableName(configuration);
        List<ViewInfo> viewInfoList = new ArrayList<>();

        try (Connection connection = ConnectionUtil.getInputConnection(configuration, new Properties())){
            ResultSet rs = connection.createStatement().executeQuery(constructAllViewsQuery(baseTableName));

            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
                System.out.println(rs.getString(4));
                String viewName = rs.getString(4);
                String tenantId = rs.getString(3);

                ResultSet viewRs = connection.createStatement().executeQuery(
                        constructViewMetadataQuery(viewName, tenantId));
                while (viewRs.next()) {
                    String schema = viewRs.getString(2);

                    ViewInfo viewInfo = new ViewInfo(
                            viewRs.getString(1),
                            viewName,
                            viewRs.getLong(3)
                    );
                    viewInfoList.add(viewInfo);
                }
            }
        } catch (SQLException e ) {
            LOGGER.error(e.getSQLState());
        }

        return viewInfoList;
    }

    public static String constructAllViewsQuery(String baseTableName) {
        String query = SELECT_ALL_VIEW_FROM_CHILD_LINK_QUERY;

        if (baseTableName.equals(RUNNING_FOR_DELETE_ALL_VIEWS_STRING)) {
            return query;
        }

        if (baseTableName.contains(".")) {
            String[] names = baseTableName.split("\\.");
            query = query + " AND TABLE_SCHEM = '" + names[0] + "'";

            baseTableName = names[1];
        }

        return query + " AND TABLE_NAME = '" + baseTableName + "'";
    }

    public static String constructViewMetadataQuery(String viewName, String tenantId) {
        String query = SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY;

        if (viewName.contains(".")) {
            String[] names = viewName.split("\\.");
            query = query + " AND TABLE_SCHEM = '" + names[0] + "'";

            viewName = names[1];
        }

        if (tenantId != null && tenantId.length() > 0) {
            query = query + " AND TENANT_ID = '" + tenantId + "'";
        }

        return query + " AND TABLE_NAME = '" + viewName + "'";
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new ViewTTLTool(), args);
        System.exit(result);
    }
}
