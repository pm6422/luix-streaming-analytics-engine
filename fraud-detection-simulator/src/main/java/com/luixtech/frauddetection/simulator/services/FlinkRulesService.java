/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.luixtech.frauddetection.simulator.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.domain.Rule;
import com.luixtech.frauddetection.simulator.model.RulePayload;
import com.luixtech.frauddetection.simulator.model.RulePayload.RuleState;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class FlinkRulesService {

    private static final ObjectMapper                  OBJECT_MAPPER = new ObjectMapper();
    private final        KafkaTemplate<String, String> kafkaTemplate;
    private final        ApplicationProperties         applicationProperties;

    public void addRule(Rule rule) {
        String payload = rule.getRulePayload();
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRules(), payload);
    }

    public void deleteRule(int ruleId) throws JsonProcessingException {
        RulePayload payload = new RulePayload();
        payload.setRuleId(ruleId);
        payload.setRuleState(RuleState.DELETE);
        String payloadJson = OBJECT_MAPPER.writeValueAsString(payload);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRules(), payloadJson);
    }
}
