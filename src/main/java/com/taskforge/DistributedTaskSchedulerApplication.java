package com.taskforge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main Spring Boot application for Distributed Task Scheduler Backend
 * 
 * Features:
 * - Task scheduling and execution
 * - Kafka-based task distribution
 * - Redis for distributed locking
 * - PostgreSQL for persistence
 */
@SpringBootApplication
@EnableScheduling
public class DistributedTaskSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedTaskSchedulerApplication.class, args);
    }

}
