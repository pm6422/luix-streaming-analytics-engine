package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.framework.exception.DataNotFoundException;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.datasource.Transaction;
import com.luixtech.frauddetection.simulator.domain.Rule;
import com.luixtech.frauddetection.simulator.model.Alert;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.services.KafkaTransactionsPusher;
import lombok.AllArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class AlertsController {

    private static final ObjectMapper            OBJECT_MAPPER = new ObjectMapper();
    private final        RuleRepository          repository;
    private final        KafkaTransactionsPusher transactionsPusher;
    private final        SimpMessagingTemplate   simpSender;
    private final        ApplicationProperties   applicationProperties;

    @GetMapping("/rules/{id}/alert")
    Alert mockAlert(@PathVariable Integer id) throws JsonProcessingException {
        Rule rule = repository.findById(id).orElseThrow(() -> new DataNotFoundException(id.toString()));
        Transaction triggeringEvent = transactionsPusher.getLastTransaction();
        String violatedRule = rule.getRulePayload();
        BigDecimal triggeringValue = triggeringEvent.getPaymentAmount().multiply(new BigDecimal(10));

        Alert alert = new Alert(rule.getId(), violatedRule, triggeringEvent, triggeringValue);
        String result = OBJECT_MAPPER.writeValueAsString(alert);
        simpSender.convertAndSend(applicationProperties.getWebSocket().getTopic().getAlerts(), result);
        return alert;
    }
}
