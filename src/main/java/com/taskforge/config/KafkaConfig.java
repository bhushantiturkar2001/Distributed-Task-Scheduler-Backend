package com.taskforge.config;

import com.taskforge.model.Task;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Configuration for TaskForge
 * Configures Kafka topics, producers, consumers, and serialization settings
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
    
    // Priority-based topics for priority queue implementation
    public static final String TASK_EXECUTE_HIGH_PRIORITY = "task.execute.high";
    public static final String TASK_EXECUTE_MEDIUM_PRIORITY = "task.execute.medium";
    public static final String TASK_EXECUTE_LOW_PRIORITY = "task.execute.low";

    /**
     * Create task execution topic (default - for backward compatibility)
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
     * Create HIGH priority task topic
     * - 2 partitions for parallel processing
     * - Replication factor 1
     * 
     * @return NewTopic configuration
     */
    @Bean
    public NewTopic taskExecuteHighPriorityTopic() {
        return TopicBuilder.name(TASK_EXECUTE_HIGH_PRIORITY)
                .partitions(2)
                .replicas(1)
                .build();
    }
    
    /**
     * Create MEDIUM priority task topic
     * - 3 partitions for parallel processing
     * - Replication factor 1
     * 
     * @return NewTopic configuration
     */
    @Bean
    public NewTopic taskExecuteMediumPriorityTopic() {
        return TopicBuilder.name(TASK_EXECUTE_MEDIUM_PRIORITY)
                .partitions(3)
                .replicas(1)
                .build();
    }
    
    /**
     * Create LOW priority task topic
     * - 2 partitions for parallel processing
     * - Replication factor 1
     * 
     * @return NewTopic configuration
     */
    @Bean
    public NewTopic taskExecuteLowPriorityTopic() {
        return TopicBuilder.name(TASK_EXECUTE_LOW_PRIORITY)
                .partitions(2)
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

    /**
     * Consumer Factory Configuration
     * Configures how tasks are deserialized from Kafka
     * 
     * @return ConsumerFactory for creating Kafka consumers
     */
    @Bean
    public ConsumerFactory<String, Task> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Kafka broker connection
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Consumer group
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "taskforge-workers");
        
        // Deserialization: String keys, JSON values
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        
        // JSON deserialization settings
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Task.class.getName());
        
        // Consumer behavior
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Kafka Listener Container Factory
     * Creates containers for @KafkaListener methods
     * 
     * @return ConcurrentKafkaListenerContainerFactory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Task> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Task> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // Concurrency: Number of consumer threads per listener
        factory.setConcurrency(3);
        
        // Error handling
        factory.setCommonErrorHandler(null); // Add custom error handler in future
        
        return factory;
    }
}
