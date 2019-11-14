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

import com.google.common.collect.Lists;
import org.antlr.runtime.CharStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.impl.MetricRegistriesImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SpanReceiver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

public class ViewTTLTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLTool.class);

    private final String GET_ALL_VIEW_TTL_QUERY = "SELECT TENANT_ID, TABLE_NAME FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private String tenantViewQuery = "SELECT VIEW_STATEMENT FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "' AND TABLE_NAME='%s'" ;

    private Map<String, List<View>> map = new HashMap<>();
    private List<View> views = new ArrayList<>();
    Configuration configuration;
    Connection connection;

    public class View {
        String tenandId;
        String tableName;
        String viewSmt;
    }

    public class SingleViewTTLTool extends Configured implements Tool {

        private String viewName;
        private JobPriority jobPriority;
        private boolean isForeground;
        private Job job;

        SingleViewTTLTool(String viewName) {
            this.viewName = viewName;
            this.isForeground = true;
        }

        @Override
        public int run(String[] args) throws Exception {
            configureJob();
            TableMapReduceUtil.initCredentials(job);
            int ret = runJob();
            return ret;
        }

        private void configureJob() throws Exception {
            this.job = Job.getInstance(getConf(),
                    "ViewTTLTool-" + viewName + "-" + System.currentTimeMillis());
            PhoenixMapReduceUtil.setInput(this.viewName, job, ViewTTLTestWritable.class);

            PhoenixConfigurationUtil.setMRJobType(job.getConfiguration(), PhoenixConfigurationUtil.MRJobType.VIEW_TTL_DELETE);

            job.setJarByClass(SingleViewTTLTool.class);
            job.setMapperClass(ViewTTLTool.EmptyViewMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
//            job.setPriority(this.jobPriority);

            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(), PhoenixConnection.class, Chronology.class,
                    CharStream.class, TransactionSystemClient.class, TransactionNotInProgressException.class,
                    ZKClient.class, DiscoveryServiceClient.class, ZKDiscoveryService.class,
                    Cancellable.class, TTransportException.class, SpanReceiver.class, TransactionProcessor.class, Gauge.class, MetricRegistriesImpl.class);
            LOGGER.info("ViewTTLTool running for: " + viewName);
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
    }

    /**
     * Empty Mapper class since range scan and condition happens as part of scanner object
     */
    public static class EmptyViewMapper
            extends Mapper<NullWritable, ViewTTLTestWritable, NullWritable, NullWritable> {

        @Override
        protected void map(NullWritable key, ViewTTLTestWritable value,
                           Context context) {
            System.out.println(value.getId() + ", " + value.getNum() + "," + value.getCol2());
        }
    }

    public static class ViewTTLTestWritable implements DBWritable {

        private String col2;
        private int id;
        private int num;

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            id = rs.getInt(1);
            num = rs.getInt(2);
            col2 = rs.getString(3);
        }

        @Override
        public void write(PreparedStatement pstmt) throws SQLException {
            pstmt.setInt(1, id);
            pstmt.setInt(2, num);
            pstmt.setString(3, col2);
        }

        public int getId() {
            return id;
        }
        public int getNum() {
            return num;
        }
        public String getCol2() {
            return col2;
        }
    }

    private void getAllViewTTLList(Connection phoenixConnection) throws SQLException {


        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(GET_ALL_VIEW_TTL_QUERY);
        while (viewRS.next()) {
            View view = new View();
            view.tenandId = viewRS.getString(1);
            view.tableName = viewRS.getString(2);
//            view.viewSmt = viewRS.getString(3);
            views.add(view);
        }

//        String ViewStatementQuery = "SELECT VIEW_STATEMENT FROM " +
//                SYSTEM_CATALOG_NAME + " WHERE " +
//                TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue()
//                + "' AND VIEW_STATEMENT IS NOT NULL AND TABLE_NAME = '%s'" ;
//
//        for (View view : views) {
//            if (view.tenandId != null) {
//                Properties props = new Properties();
//                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, view.tenandId);
//                Connection tenantConnection = ConnectionUtil.getInputConnection(configuration, new Properties());
//                viewRS = tenantConnection.createStatement().executeQuery(String.format(ViewStatementQuery, view.tableName));
//
//                if (viewRS.next()) {
//                    view.viewSmt = viewRS.getNString(1);
//                }
//            }
//        }

    }

    private void runDeleteViewTTLJobs() throws Exception {
        String viewNames = "";
        SingleViewTTLTool tool = new SingleViewTTLTool("");
        for (View view : views) {
            viewNames = viewNames + view.tableName + ",";
        }

        viewNames = viewNames.substring(0, viewNames.length() - 1);
        final List<String> args = Lists.newArrayList();
        Configuration config = HBaseConfiguration.addHbaseResources(getConf());
        config.set(PhoenixConfigurationUtil.MAPREDUCE_SPLIT_BY_VIEW_TTL, viewNames);

        tool.setConf(config);

        tool.run(args.toArray(new String[0]));
    }

    @Override
    public int run(String[] args) throws Exception {
        connection = null;
        try {

            configuration = HBaseConfiguration.addHbaseResources(getConf());

//            parseOptions(args);

            connection = ConnectionUtil.getInputConnection(configuration, new Properties());
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            getAllViewTTLList(phoenixConnection);
            runDeleteViewTTLJobs();
        } catch (Exception e) {
//                    printHelpAndExit(e.getMessage(), getOptions());
            return -1;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

        return 0;
    }

    public static Connection buildTenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new ViewTTLTool(), args);
        System.exit(result);
    }
}