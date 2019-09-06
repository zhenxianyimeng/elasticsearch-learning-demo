package com.zx.learn.es.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ElasticsearchClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchClientApplication.class, args);
    }

}
