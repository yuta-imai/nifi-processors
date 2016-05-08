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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "JSONAttributes", description = "JSON representation of Attributes")
@CapabilityDescription("The processor which converts text data to JSON with Regex")
public class ConvertToJSONWithRegex extends AbstractProcessor {

    private static final String APPLICATION_JSON = "application/json";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to JSON").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();

    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("Regular Expression")
            .required(false)
            .description("Regular expression with named keys to extract values from text")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REGEX);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if ( original == null ) {
            return;
        }
        final String regex = context.getProperty(REGEX).toString();
        //TODO Should be moved to initialization phase.
        final Set keys = getNamedGroupCandidates(regex);
        //TODO Should validate that regex includes keys.
        final Pattern pattern = Pattern.compile(regex);

        try {
            FlowFile result = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        String line = in.toString();
                        Matcher matches = pattern.matcher(line);
                        if(matches.find()) {
                            Map parsed = parse(keys, matches);
                        }else {
                            return;
                        }
                    }
                }
            });
            result = session.putAttribute(result, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            session.transfer(result,REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }

    private static Set<String> getNamedGroupCandidates(String regex) {
        Set<String> namedGroups = new TreeSet<String>();

        Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);

        while (m.find()) {
            namedGroups.add(m.group(1));
        }
        return namedGroups;
    }

    private static Map parse(Set keys, Matcher matches) {
        HashMap<String, String> map = new HashMap<String, String>();
        for(Object key: keys){
            map.put(key.toString(), matches.group(key.toString()));
        }
        return map;
    }
}
