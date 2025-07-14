package com.rpc_kafka.worker.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class WorkerListener {
    @Autowired
    private KafkaTemplate<String, String> template;

    @KafkaListener(topics = "requests")
    public void process(@Payload String message, @Headers MessageHeaders headers) {
        String[] parts = message.split(":");
        String procedure = parts[0];
        String[] args = parts[1].split(",");
        String result = "";
        if ("add".equals(procedure)) {
            int sum = Integer.parseInt(args[0]) + Integer.parseInt(args[1]);
            result = String.valueOf(sum);
        }

        byte[] replyTopicBytes = headers.get(KafkaHeaders.REPLY_TOPIC, byte[].class);
        String replyTopic = new String(replyTopicBytes);

        Message<String> response = MessageBuilder.withPayload(result)
                .setHeader(KafkaHeaders.TOPIC, replyTopic)
                .setHeader(KafkaHeaders.CORRELATION_ID, headers.get(KafkaHeaders.CORRELATION_ID))
                .build();
        template.send(response);
    }
}