package com.codehack;

import org.springframework.boot.Banner.Mode;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ComponentScan
@EnableScheduling
public class SpringQuartzApp {

    public static void main(String[] args) {
        new SpringApplicationBuilder(SpringQuartzApp.class).bannerMode(Mode.OFF).run(args);
    }
}
