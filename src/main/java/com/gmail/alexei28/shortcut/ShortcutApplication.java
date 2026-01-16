package com.gmail.alexei28.shortcut;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ShortcutApplication {
    private static final Logger logger = LoggerFactory.getLogger(ShortcutApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ShortcutApplication.class, args);
        logger.info("Java version: {}, Java vendor: {}", System.getProperty("java.version"), System.getProperty("java.vendor"));
	}

}
