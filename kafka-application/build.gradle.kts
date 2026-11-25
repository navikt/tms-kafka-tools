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
    api(libs.jackson.databind)
    api(libs.kafka.clients)
    api(libs.ktor.server.core)
    api(libs.ktor.utils)
    implementation(libs.jackson.moduleKotlin)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.ktor.events)
    implementation(libs.ktor.http)
    implementation(libs.ktor.server.cio)
    implementation(libs.ktor.server.metricsMicrometer)
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registryPrometheus)
    implementation(libs.prometheus.metricsCore)
    implementation(libs.prometheus.metricsExpositionTextformats)
    implementation(libs.prometheus.metricsModel)
    implementation(libs.slf4j.api)
    implementation(libs.tms.commonLib.teamLogger)
    runtimeOnly(libs.jackson.datatypeJsr310)
    runtimeOnly(libs.logback.classic)

    testImplementation(libs.awaitility)
    testImplementation(libs.junit.api)
    testImplementation(libs.kotest.assertionsCore)
    testImplementation(libs.kotest.assertionsShared)
    testImplementation(libs.ktor.client.core)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly(libs.junit.engine)
    testRuntimeOnly(libs.junit.launcher)
    testRuntimeOnly(libs.ktor.client.apache5)
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
            artifactId = "kafka-application"
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
