package com.cihangir.kafka.controller;

import com.cihangir.kafka.model.MessageDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;


@RestController
@RequestMapping("api/producer")
public class ProducerController {
    @Value(value = "${spring.kafka.template.first-topic}")
    private String firstTopicName;

    @Value(value = "${spring.kafka.template.second-topic}")
    private String secondTopicName;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    @GetMapping("/send-message-to-first-topic")
    public ResponseEntity<String> sendMessageToFirstTopic() {
        final String message = "Bu bir test log mesajıdır " + LocalDateTime.now().toString();

        //MessageEntity messageEntity = new MessageEntity("test", LocalDateTime.now());
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(firstTopicName, message);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("Send message with offset : " + result.getRecordMetadata().offset());
                System.out.println("Send message with topic : "+result.getRecordMetadata().topic());
                System.out.println("Send message with partition : "+result.getRecordMetadata().partition());

                System.out.println(result.getProducerRecord());



            }
        });
        return ResponseEntity.ok(message);

    }


    @GetMapping("/send-message-to-second-topic")
    public ResponseEntity<String> sendMessageToSecondTopic() {
        final String message = "İmkanın sınırını görmek için, imkansızı denemek lazım.";

        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(secondTopicName, new MessageDto(message));
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("Send message with offset : " + result.getRecordMetadata().offset());
                System.out.println("Send message with topic : "+result.getRecordMetadata().topic());
                System.out.println("Send message with partition : "+result.getRecordMetadata().partition());

                System.out.println(result.getProducerRecord());

            }
        });
        return ResponseEntity.ok(message);

    }


}
