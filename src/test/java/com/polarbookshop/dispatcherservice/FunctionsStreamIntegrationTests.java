package com.polarbookshop.dispatcherservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class FunctionsStreamIntegrationTests {

    @Autowired
    private InputDestination input;  // Represents the input binding (e.g. packlabel-in-0)

    @Autowired
    private OutputDestination output;  // Represents the output binding (e.g. packlabel-out-0)

    @Autowired
    private ObjectMapper objectMapper;  // Used to deserialize JSON message payloads

    @Test
    void whenOrderAcceptedThenDispatched() throws IOException {
        long orderId = 121L;

        Message<OrderAcceptedMessage> inputMessage = MessageBuilder
                .withPayload(new OrderAcceptedMessage(orderId))
                .build();

        Message<OrderDispatchedMessage> expectedOutputMessage = MessageBuilder
                .withPayload(new OrderDispatchedMessage(orderId))
                .build();

        input.send(inputMessage);

        byte[] payload = output.receive().getPayload();
        OrderDispatchedMessage actualOutput = objectMapper.readValue(payload, OrderDispatchedMessage.class);

        assertThat(actualOutput).isEqualTo(expectedOutputMessage.getPayload());
    }
}