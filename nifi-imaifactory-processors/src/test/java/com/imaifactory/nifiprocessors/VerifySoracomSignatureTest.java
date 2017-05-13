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
import java.util.HashMap;
import java.util.Map;


public class VerifySoracomSignatureTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(VerifySoracomSignature.class);
    }

    @Test
    public void testProcessor_right_signature() {

        testRunner.setProperty(VerifySoracomSignature.SECRET, "topsecret");
        testRunner.setProperty(VerifySoracomSignature.SORACOM_SIGNATURE_VERSION, "20151001");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile ff = session.create();
        Map<String,String>  attributes = new HashMap<>();
        attributes.put("http.headers.x-soracom-signature", "10aba09ab9c52a5e738b3bf98242075fe6f3f5d4ce55d323383870f1bc72b29c");
        attributes.put("http.headers.x-soracom-signature-version","20151001");
        attributes.put("http.headers.x-soracom-timestamp","1494631852102");
        attributes.put("http.headers.x-soracom-imsi","440103114938318");
        attributes.put("http.headers.x-soracom-imei","359675070019530");
        FlowFile modifiedFF = session.putAllAttributes(ff,attributes);

        testRunner.enqueue(modifiedFF);
        testRunner.run();
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_SUCCESS, 1);
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_FAILURE, 0);
        testRunner.getProcessor();

    }

    @Test
    public void testProcessor_invalid_signature() {

        testRunner.setProperty(VerifySoracomSignature.SECRET, "topsecret");
        testRunner.setProperty(VerifySoracomSignature.SORACOM_SIGNATURE_VERSION, "20151001");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile ff = session.create();
        Map<String,String>  attributes = new HashMap<>();
        attributes.put("http.headers.x-soracom-signature", "INVALID_SIGNATURE");
        attributes.put("http.headers.x-soracom-signature-version","20151001");
        attributes.put("http.headers.x-soracom-timestamp","1494631852102");
        attributes.put("http.headers.x-soracom-imsi","440103114938318");
        attributes.put("http.headers.x-soracom-imei","359675070019530");
        FlowFile modifiedFF = session.putAllAttributes(ff,attributes);

        testRunner.enqueue(modifiedFF);
        testRunner.run();
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ConvertToJSONWithRegex.REL_FAILURE, 1);
        testRunner.getProcessor();

    }

}
