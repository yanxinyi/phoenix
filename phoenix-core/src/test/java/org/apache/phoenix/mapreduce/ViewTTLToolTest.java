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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ViewTTLToolTest {
    @Test
    public void testParseInput() {
        ViewTTLTool tool;

        tool = new ViewTTLTool();
        tool.parseOptions(new String[] {});
        assertEquals("NORMAL", tool.getJobPriority());

        tool.parseOptions(new String[] {"-t", "table1"});
        assertEquals("NORMAL", tool.getJobPriority());

        tool.parseOptions(new String[] {"-t", "table1", "-p", "0"});
        assertEquals("VERY_HIGH", tool.getJobPriority());

        tool.parseOptions(new String[] {"-t", "table1", "-p", "-1"});
        assertEquals("NORMAL", tool.getJobPriority());

        tool.parseOptions(new String[] {"-t", "table1", "-p", "DSAFDAS"});
        assertEquals("NORMAL", tool.getJobPriority());
    }

//    @Test
//    public void testQuery() {
//
//    }
}
