plugins {
    java
    id("org.springframework.boot") version "4.0.1"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.gmail.alexei28"
version = "0.0.1-SNAPSHOT"
description = "Shortcut project"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}


dependencies {
    implementation("org.apache.kafka:kafka-clients")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.json:json:20250107")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
