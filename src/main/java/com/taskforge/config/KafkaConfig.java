package com.taskforge.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Configuration for TaskForge
 * Configures Kafka topics, producers, and serialization settings
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Kafka Topic Names
     */
    public static final String TASK_EXECUTE_TOPIC = "task.execute";
    public static final String TASK_DEAD_LETTER_TOPIC = "task.dead";

    /**
     * Create task execution topic
     * - 3 partitions for parallel processing
     * - Replication factor 1 (single broker setup)
     * 
     * @return NewTopic configuration
     */
    @Bean
    public NewTopic taskExecuteTopic() {
        return TopicBuilder.name(TASK_EXECUTE_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Create dead letter queue topic for permanently failed tasks
     * - 1 partition (DLQ doesn't need parallelism)
     * - Replication factor 1
     * 
     * @return NewTopic configuration
     */
    @Bean
    public NewTopic taskDeadLetterTopic() {
        return TopicBuilder.name(TASK_DEAD_LETTER_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Producer Factory Configuration
     * Configures how tasks are serialized and sent to Kafka
     * 
     * @return ProducerFactory for creating Kafka producers
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Kafka broker connection
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Serialization: String keys, JSON values
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Reliability settings
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");  // Wait for all replicas
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);   // Retry on failure
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Idempotence: Prevent duplicate messages
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Batching for efficiency
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        
        // Compression
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * KafkaTemplate Bean
     * Primary interface for sending messages to Kafka
     * 
     * @return KafkaTemplate configured with producer factory
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
