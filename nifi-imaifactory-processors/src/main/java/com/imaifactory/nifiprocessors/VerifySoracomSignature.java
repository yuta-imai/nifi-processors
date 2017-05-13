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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"soracom", "signature", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Verify SORACOM Beam's signature")
public class VerifySoracomSignature extends AbstractProcessor {

    protected static final String SORACOM_SIGNATURE_VERSION_1 = "20151001";

    static final PropertyDescriptor SORACOM_SIGNATURE_VERSION = new PropertyDescriptor.Builder()
            .name("Version of SORACOM signature")
            .allowableValues(SORACOM_SIGNATURE_VERSION_1)
            .required(true)
            .defaultValue(SORACOM_SIGNATURE_VERSION_1)
            .build();

    static final PropertyDescriptor SECRET = new PropertyDescriptor.Builder()
            .name("secret")
            .required(true)
            .description("SORACOM Beam's shared secret.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private List<PropertyDescriptor> descriptors;

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully verified soracom signature").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to verify soracom signature").build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SECRET);
        descriptors.add(SORACOM_SIGNATURE_VERSION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if ( original == null ) {
            return;
        }

        Map<String, String> options = new HashMap<>();
        options.put("secret",context.getProperty(SECRET).toString());
        options.put("signature_version",context.getProperty(SORACOM_SIGNATURE_VERSION).toString());

        if(verifySignature(original,options)) {
            session.transfer(original, REL_SUCCESS);
        } else {
            session.transfer(original, REL_FAILURE);
        }
    }

    private boolean verifySignature(FlowFile flowFile,Map<String,String> options) {
        Map<String, String> attributes = flowFile.getAttributes();
        if ( !attributes.containsKey("http.headers.x-soracom-signature") ||
                !attributes.containsKey("http.headers.x-soracom-timestamp") ||
                !attributes.containsKey("http.headers.x-soracom-signature-version") ||
                !attributes.containsKey("http.headers.x-soracom-imsi")
                ) {
            return false;
        }


        String stringToSign;
        try {
            stringToSign = buildStringToSign(flowFile,options);
        } catch(ProcessException e){
            return false;
        }

        if(compareSignature(stringToSign,
                attributes.get("http.headers.x-soracom-signature"),
                attributes.get("http.headers.x-soracom-signature-version"))) {
            return true;
        } else {
            return false;
        }
    }

    private String buildStringToSign(FlowFile flowFile,Map<String,String> options) throws ProcessException {

        Map<String, String> attributes = flowFile.getAttributes();

        String stringToSign = options.get("secret");

        if(attributes.containsKey("http.headers.x-soracom-imei")) {
            stringToSign = stringToSign + "x-soracom-imei=" + attributes.get("http.headers.x-soracom-imei");
        }

        stringToSign = stringToSign + "x-soracom-imsi=" + attributes.get("http.headers.x-soracom-imsi");

        stringToSign = stringToSign + "x-soracom-timestamp=" + attributes.get("http.headers.x-soracom-timestamp");

        return stringToSign;

    }

    private boolean compareSignature (String stringToSign, String signature, String signatureVersion) {
        String hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(stringToSign);
        return hash.equals(signature);
    }
}
