import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    alias(libs.plugins.kotlin)
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }
    mavenLocal()
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    api(libs.ktor.server.core)
    api(libs.ktor.utils)
    implementation(project(":kafka-application"))
    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.moduleKotlin)
    implementation(libs.kafka.clients)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.ioCore)
    implementation(libs.ktor.http)
    implementation(libs.ktor.io)
    implementation(libs.ktor.server.auth)
    runtimeOnly(libs.jackson.datatypeJsr310)

    testImplementation(libs.awaitility)
    testImplementation(libs.junit.api)
    testImplementation(libs.kotest.assertionsCore)
    testImplementation(libs.ktor.client.core)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.tms.ktorTokenSupport.entraId.verificationMock)
    testRuntimeOnly(libs.junit.engine)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.ktor.client.cio)
}

tasks {
    withType<Test> {
        useJUnitPlatform()
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
    }
}

val libraryVersion: String = providers.gradleProperty("lib_version")
    .orElse("latest-local")
    .get()

publishing {
    repositories{
        mavenLocal()
        maven {
            url = uri("https://maven.pkg.github.com/navikt/tms-kafka-tools")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

    publications {
        create<MavenPublication>("gpr") {
            groupId = "no.nav.tms.kafka"
            artifactId = "kafka-message-replay"
            version = libraryVersion
            from(components["java"])

            val sourcesJar = tasks.register<Jar>("sources") {
                archiveClassifier.set("sources")
                from(sourceSets.main.get().allSource)
            }

            artifact(sourcesJar)
        }
    }
}
