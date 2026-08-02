package com.taskforge.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Value("${server.port:8080}")
    private int serverPort;

    @Bean
    public OpenAPI taskForgeOpenAPI() {
        Server localServer = new Server();
        localServer.setUrl("http://localhost:" + serverPort + "/api");
        localServer.setDescription("Local Development Server");

        Contact contact = new Contact();
        contact.setName("TaskForge Development Team");
        contact.setEmail("dev@taskforge.com");

        License license = new License()
                .name("MIT License")
                .url("https://opensource.org/licenses/MIT");

        Info info = new Info()
                .title("TaskForge - Distributed Task Scheduler API")
                .version("1.0.0")
                .description("Production-grade distributed task scheduling system with Kafka-based processing, " +
                        "Redis distributed locking, and PostgreSQL persistence. " +
                        "Supports one-time and recurring tasks with cron expressions.")
                .contact(contact)
                .license(license);

        return new OpenAPI()
                .info(info)
                .servers(List.of(localServer));
    }
}
