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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class ConvertToJSONWithRegexTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConvertToJSONWithRegex.class);
    }

    @Test
    public void testProcessor1() {
        testRunner.setProperty(ConvertToJSONWithRegex.REGEX, "(?<word>test)(?<number>[0-9]+).*");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile ff = session.create();
        ff = session.write(ff, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                try (OutputStream outputStream = new BufferedOutputStream(out)) {
                    outputStream.write("atest123abc".getBytes());
                }
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_MATCH, 1);
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_UNMATCH, 0);
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_FAILURE, 0);
        testRunner.getProcessor();

    }

}
