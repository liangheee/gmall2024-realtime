package com.liangheee.gmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author liangheee
 * * @date 2024-12-09
 */
@SpringBootApplication
@MapperScan(basePackages = "com.liangheee.gmall.publisher.mapper")
public class Gmall2024PublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(Gmall2024PublisherApplication.class,args);
    }
}
