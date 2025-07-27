package com.luketn.javawod;

import org.springframework.boot.SpringApplication;

public class TestJavaWodApplication {

    public static void main(String[] args) {
        SpringApplication.from(JavaWodApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
