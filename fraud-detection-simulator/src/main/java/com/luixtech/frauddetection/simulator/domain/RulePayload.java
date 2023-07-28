package com.luixtech.frauddetection.simulator.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.common.dto.Rule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RulePayload {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    private String  rulePayload;

    public RulePayload(String rulePayload) {
        this.rulePayload = rulePayload;
    }

    public static RulePayload fromRule(Rule rule) {
        RulePayload payload = new RulePayload();
        payload.setId(rule.getRuleId());
        try {
            payload.setRulePayload(OBJECT_MAPPER.writeValueAsString(rule));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return payload;
    }

    public Rule toRule() {
        try {
            return OBJECT_MAPPER.readValue(rulePayload, Rule.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
