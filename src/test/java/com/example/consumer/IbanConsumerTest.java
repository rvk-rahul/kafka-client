package com.example.consumer;

import com.example.kafka.consumer.IbanConsumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.mockito.Mockito.*;

class IbanConsumerTest {

    @Mock
    private KafkaProducer<String, String> producer;

    @Mock
    private ObjectMapper objectMapper;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testProcessIbans_withValidData() throws JsonProcessingException {
        // Arrange
        Set<String> processedIbans = new HashSet<>();
        Set<String> blacklistedIbans = new HashSet<>();
        List<String> ibans = List.of("DE1234567890", "GB9876543210");

        when(objectMapper.writeValueAsString(any())).thenReturn("{\"iban\":\"DE1234567890\"}", "{\"iban\":\"GB9876543210\"}");

        // Act
        IbanConsumer.processIbans(producer, processedIbans, objectMapper, ibans, blacklistedIbans);

        // Assert
        verify(producer, times(2)).send(producerRecordCaptor.capture());
        List<ProducerRecord<String, String>> capturedRecords = producerRecordCaptor.getAllValues();

        // Überprüfen des ersten Records (Whitelist)
        ProducerRecord<String, String> firstRecord = capturedRecords.get(0);
        assert "json-verify-whitelisted".equals(firstRecord.topic());
        assert "{\"iban\":\"DE1234567890\"}".equals(firstRecord.value());

        // Überprüfen des zweiten Records (Blacklist)
        ProducerRecord<String, String> secondRecord = capturedRecords.get(1);
        assert "json-verify-blacklisted".equals(secondRecord.topic());
        assert "{\"iban\":\"GB9876543210\"}".equals(secondRecord.value());

        verify(objectMapper, times(2)).writeValueAsString(any());
        assert blacklistedIbans.contains("GB9876543210");
    }

    @Test
    void testSendBlacklistedIbansToOutTopic() throws JsonProcessingException {
        // Arrange
        Set<String> blacklistedIbans = new HashSet<>();
        blacklistedIbans.add("GB9876543210");

        when(objectMapper.writeValueAsString(any())).thenReturn("[\"GB9876543210\"]");

        // Act
        IbanConsumer.sendBlacklistedIbansToOutTopic(producer, objectMapper, blacklistedIbans);

        // Assert
        verify(producer).send(producerRecordCaptor.capture());
        ProducerRecord<String, String> capturedRecord = producerRecordCaptor.getValue();
        assert "out-topic".equals(capturedRecord.topic());
        assert "[\"GB9876543210\"]".equals(capturedRecord.value());

        verify(objectMapper).writeValueAsString(blacklistedIbans);
        assert blacklistedIbans.isEmpty(); // Die Liste sollte nach dem Senden leer sein
    }

    @Test
    void testDetermineTopic_forWhitelist() {
        // Act & Assert
        String topic = IbanConsumer.determineTopic("DE1234567890");
        assert "json-verify-whitelisted".equals(topic);
    }

    @Test
    void testDetermineTopic_forBlacklist() {
        // Act & Assert
        String topic = IbanConsumer.determineTopic("GB9876543210");
        assert "json-verify-blacklisted".equals(topic);
    }
}
