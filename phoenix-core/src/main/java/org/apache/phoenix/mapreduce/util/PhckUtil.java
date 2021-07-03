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

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;

public class PhckUtil {
    public static final String EMPTY_COLUMN_VALUE = " ";
    public static final String NULL_VALUE = "null";

    public enum PHCK_STATE {
        ORPHAN_ROW,
        ORPHAN_VIEW,
        MISMATCH_COLUMN_COUNT,
        INVALID_SYSTEM_TABLE_LINK,
        INVALID_TABLE,
        INVALID_CHILD_LINK_ROW,
        VALID,
    }

    public enum PHCK_ROW_RESOURCE {
        CATALOG,
        CHILD_LINK,
    }

    public static final String BASE_SELECT_QUERY = "SELECT " +
            TENANT_ID + ", " + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," +
            COLUMN_FAMILY + "," + LINK_TYPE + "," + COLUMN_COUNT + "," + TABLE_TYPE + "," +
            INDEX_STATE + "," + INDEX_TYPE + "," + VIEW_TYPE + "," + COLUMN_QUALIFIER_COUNTER +
            " FROM " + SYSTEM_CATALOG_NAME;

    public static final String SELECT_CHILD_LINK_QUERY = "SELECT " +
            TENANT_ID + ", " + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," +
            COLUMN_FAMILY + "," + LINK_TYPE +
            " FROM " + SYSTEM_CHILD_LINK_NAME;

    public static Options getOptions() {
        final Options options = new Options();
        return options;
    }

    public static void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    public static void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    public static String constructTableName(String tableName, String tableSchema, String tenantId) {

        if (tableSchema != null && tableSchema.length() != 0 &&
                !tableSchema.equals(EMPTY_COLUMN_VALUE) &&
                !tableSchema.equals(NULL_VALUE))  {
            tableName = tableSchema + "." + tableName;
        }
        if (tenantId != null && tenantId.length() != 0 &&
                !tenantId.equals(EMPTY_COLUMN_VALUE) &&
                !tenantId.equals(NULL_VALUE)) {
            tableName = tenantId + "," + tableName;
        }

        return tableName;
    }

    public static void closeConnection(Connection connection, Logger LOGGER) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to close connection: ", e);
            throw new RuntimeException("Failed to close connection with exception: ", e);
        }
    }
}
