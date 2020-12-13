package com.cihangir.kafka.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    @Value(value = "${spring.kafka.template.first-topic}")
    private String firstTopicName;

    @Value(value = "${spring.kafka.template.second-topic}")
    private String secondTopicName;


    @Autowired
    private Consumer<String, Object> manualConsumer;


    @GetMapping("/manual")
    public ResponseEntity<?> getMessagesManually() {
        //<---- By the way, best practice is; call it from service as method.
        //We can fetch them by partition.

        TopicPartition partition = new TopicPartition(firstTopicName, 0);
        manualConsumer.assign(Collections.singletonList(partition)); //subscribe partitions

        //We will fetch them from beginning with offset 0.
        manualConsumer.seek(partition, 0); //Search messages in partitions

        // Timeout to find messages is 1000ms
        ConsumerRecords<String, Object> records = manualConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, Object> record : records) {
            System.out.println(record);
        }
        manualConsumer.unsubscribe(); //close listener.
        return ResponseEntity.ok(StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList()));
    }


}
