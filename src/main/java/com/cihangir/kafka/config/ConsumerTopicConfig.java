package com.cihangir.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@EnableKafka// it is required on the configuration class to enable detection of @KafkaListener.
@Configuration
public class ConsumerTopicConfig {

    @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapAddress;

    public ConsumerFactory<String, Object> consumerFactory(String groupId) {
        return consumerFactory(groupId, null);
    }

    //<Key,Value> pair should match with producer factory.
    //groupId -> consumerGroupId
    public ConsumerFactory<String, Object> consumerFactory(String groupId, String isolationLevel)
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        //Different consumer groups can share the same record or same consumer-group can share the work.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //It has same type with producer key.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //In default, consumer does not wait to producer-done-commit-state. -> unread_committed.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, Objects.requireNonNullElse(isolationLevel, ConsumerConfig.DEFAULT_ISOLATION_LEVEL));

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jsonDeserializer);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> firstKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupId1"));
        factory.setConcurrency(3);
        factory.setAutoStartup(true); //It will be automatically started when application is up.
        return factory;
    }




}
