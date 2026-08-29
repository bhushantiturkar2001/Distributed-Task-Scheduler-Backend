package com.taskforge.kafka;

import com.taskforge.config.KafkaConfig;
import com.taskforge.model.Task;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * TaskProducer - Publishes tasks to Kafka topics
 * Handles serialization and asynchronous message sending
 */
@Component
@Slf4j
public class TaskProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TaskProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a task to the execution queue with priority-based routing.
     * Routes tasks to priority-specific Kafka topics:
     * - HIGH priority -> task.execute.high
     * - MEDIUM priority -> task.execute.medium
     * - LOW priority -> task.execute.low
     * 
     * Uses task ID as the Kafka message key for consistent partitioning
     * 
     * @param task The task to be published
     */
    public void publishTask(Task task) {
        String taskId = task.getId().toString();
        String topic = getTopicForPriority(task.getPriority());
        
        log.info("Publishing task to Kafka - ID: {}, Name: {}, Priority: {}, Topic: {}", 
                taskId, task.getName(), task.getPriority(), topic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate
                .send(topic, taskId, task);

        // Async callback for success/failure
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Task published successfully - ID: {}, Priority: {}, Topic: {}, Partition: {}, Offset: {}", 
                        taskId, 
                        task.getPriority(),
                        topic,
                        result.getRecordMetadata().partition(), 
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish task - ID: {}, Priority: {}, Error: {}", 
                        taskId, task.getPriority(), ex.getMessage(), ex);
            }
        });
    }
    
    /**
     * Get Kafka topic name based on task priority.
     * 
     * @param priority Task priority
     * @return Kafka topic name
     */
    private String getTopicForPriority(Task.TaskPriority priority) {
        if (priority == null) {
            priority = Task.TaskPriority.MEDIUM; // Default to MEDIUM if null
        }
        
        return switch (priority) {
            case HIGH -> KafkaConfig.TASK_EXECUTE_HIGH_PRIORITY;
            case MEDIUM -> KafkaConfig.TASK_EXECUTE_MEDIUM_PRIORITY;
            case LOW -> KafkaConfig.TASK_EXECUTE_LOW_PRIORITY;
        };
    }

    /**
     * Publish a task to the execution queue synchronously
     * Blocks until the message is sent or fails
     * 
     * @param task The task to be published
     * @throws Exception if message sending fails
     */
    public void publishTaskSync(Task task) throws Exception {
        String taskId = task.getId().toString();
        
        log.info("Publishing task synchronously - ID: {}, Name: {}", taskId, task.getName());

        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(KafkaConfig.TASK_EXECUTE_TOPIC, taskId, task)
                    .get(); // Blocking call

            log.info("Task published successfully (sync) - ID: {}, Partition: {}, Offset: {}", 
                    taskId, 
                    result.getRecordMetadata().partition(), 
                    result.getRecordMetadata().offset());
        } catch (Exception e) {
            log.error("Failed to publish task synchronously - ID: {}, Error: {}", 
                    taskId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Publish a failed task to the Dead Letter Queue
     * Used for tasks that exceeded max retry attempts
     * 
     * @param task The task to be moved to DLQ
     */
    public void publishToDeadLetterQueue(Task task) {
        String taskId = task.getId().toString();
        
        log.warn("Publishing task to Dead Letter Queue - ID: {}, Name: {}, Retry Count: {}", 
                taskId, task.getName(), task.getRetryCount());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate
                .send(KafkaConfig.TASK_DEAD_LETTER_TOPIC, taskId, task);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Task moved to DLQ successfully - ID: {}, Offset: {}", 
                        taskId, result.getRecordMetadata().offset());
            } else {
                log.error("Failed to move task to DLQ - ID: {}, Error: {}", 
                        taskId, ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publish a task with custom partition key
     * Useful for routing tasks to specific worker partitions
     * 
     * @param task The task to be published
     * @param partitionKey Custom partition key for routing
     */
    public void publishTaskWithKey(Task task, String partitionKey) {
        log.info("Publishing task with custom key - ID: {}, Key: {}", 
                task.getId(), partitionKey);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate
                .send(KafkaConfig.TASK_EXECUTE_TOPIC, partitionKey, task);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Task published with custom key - ID: {}, Key: {}, Partition: {}", 
                        task.getId(), partitionKey, result.getRecordMetadata().partition());
            } else {
                log.error("Failed to publish task with custom key - ID: {}, Key: {}, Error: {}", 
                        task.getId(), partitionKey, ex.getMessage(), ex);
            }
        });
    }

    /**
     * Send a generic message to any topic
     * Useful for future extensions (metrics, events, etc.)
     * 
     * @param topic Kafka topic name
     * @param key Message key
     * @param message Message payload
     */
    public void sendMessage(String topic, String key, Object message) {
        log.debug("Sending message to topic: {}, key: {}", topic, key);

        kafkaTemplate.send(topic, key, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Message sent to topic: {}, partition: {}", 
                                topic, result.getRecordMetadata().partition());
                    } else {
                        log.error("Failed to send message to topic: {}, error: {}", 
                                topic, ex.getMessage(), ex);
                    }
                });
    }
}
