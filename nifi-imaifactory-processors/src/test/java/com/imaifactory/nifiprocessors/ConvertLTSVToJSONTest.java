/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.imaifactory.nifiprocessors;

import am.ik.ltsv4j.LTSV;
import am.ik.ltsv4j.LTSVFormatter;
import am.ik.ltsv4j.LTSVParser;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;


public class ConvertLTSVToJSONTest {

    private TestRunner testRunner;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final LTSVParser ltsvParser = LTSV.parser();
    private static final LTSVFormatter ltsvFormatter = LTSV.formatter();

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConvertLTSVToJSON.class);
    }

    @Test
    public void testProcessor1() {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile ff = session.create();
        final HashMap<String,String> map = new HashMap<String,String>();
        map.put("key","value");

        ff = session.write(ff, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                try (OutputStream outputStream = new BufferedOutputStream(out)) {
                    outputStream.write(ltsvFormatter.formatLine(map).getBytes());
                }
            }
        });
        testRunner.enqueue(ff);
        testRunner.run();
        testRunner.assertTransferCount(ConvertLTSVToJSON.REL_SUCCESS, 1);
        testRunner.assertTransferCount(ConvertLTSVToJSON.REL_FAILURE, 0);

    }

    @Test
    public void testProcessor2() {
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile ff = session.create();

        ff = session.write(ff, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                try (OutputStream outputStream = new BufferedOutputStream(out)) {
                    outputStream.write("test".getBytes());
                }
            }
        });
        testRunner.enqueue(ff);
        testRunner.run();
        testRunner.assertTransferCount(ConvertLTSVToJSON.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ConvertLTSVToJSON.REL_FAILURE, 1);

    }

}
