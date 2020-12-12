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


    /*
    public void listenFirstTopicWithDetails(ConsumerRecord<String, MessageDto> consumerRecord,
                                            @Payload MessageDto messageEntity,
                                            @Header(KafkaHeaders.GROUP_ID) String groupId,
                                            @Header(KafkaHeaders.OFFSET) int offset,
                                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    {
        System.out.println("Received message with below details:");
        System.out.println(consumerRecord); //We can also reach to all details from consumerRecord.
        System.out.println(messageEntity); //This is value.
        System.out.println(groupId); //This will be groupId1.
        System.out.println(offset); //Current record offset.
        System.out.println(partition); //We used only one partition, so this will be 0 for each message.
    }*/

    @KafkaListener(
            topics = "${spring.kafka.template.second-topic}",
            groupId = "groupId1"
    )
    public void listen(@Payload MessageDto message) {
        log.info("Message received.. MessageID : {} Message: {} Date : {}",
                message.getMessage(),message.getCreatedDate());
    }



}
