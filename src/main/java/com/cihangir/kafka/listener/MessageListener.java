package com.cihangir.kafka.listener;

import com.cihangir.kafka.model.MessageDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {


    private final Logger log = LoggerFactory.getLogger(MessageListener.class);


    @KafkaListener(
            topics = "${spring.kafka.template.second-topic}",
            groupId = "groupId1"
    )
    public void listen(@Payload MessageDto message) {
        log.info("Message received.. MessageID : {} Message: {} Date : {}",
                message.getMessage(), message.getCreatedDate());
    }


}
