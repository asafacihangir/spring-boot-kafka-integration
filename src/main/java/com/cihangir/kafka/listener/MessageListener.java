package com.cihangir.kafka.listener;

import com.cihangir.kafka.model.MessageDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {


    private final Logger log = LoggerFactory.getLogger(MessageListener.class);

    @KafkaListener(
            topics = "${spring.kafka.template.second-topic}",
            groupId = "groupId1"
    )
    public void listen(ConsumerRecord<String, MessageDto> consumerRecord,
                       @Payload MessageDto message,
                       @Header(KafkaHeaders.OFFSET) int offset,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Message received.. MessageID : {} Message: {} Date : {}",
                message.getMessage(),message.getCreatedDate());
        System.out.println(consumerRecord); //We can also reach to all details from consumerRecord.
        System.out.println(offset); //Current record offset.
        System.out.println(partition); //We used only one partition, so this will be 0 for each message.

    }



}
