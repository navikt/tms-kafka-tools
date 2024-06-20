import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    kotlin("jvm").version(Kotlin.version)
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
    mavenLocal()
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    constraints {
        implementation("org.apache.zookeeper:zookeeper:[3.9.2,)") {
            because("CWE-639: Authorization Bypass Through User-Controlled Key")
        }
    }

    implementation(JacksonDatatype.datatypeJsr310)
    implementation(JacksonDatatype.moduleKotlin)
    implementation(Micrometer.registryPrometheus)
    implementation(Kotlinx.coroutines)
    implementation(KotlinLogging.logging)
    implementation(Kafka.kafka_2_12)
    implementation(Ktor.Server.core)
    implementation(Ktor.Server.cio)
    implementation(Ktor.Server.metricsMicrometer)
    implementation(Prometheus.simpleClient)
    implementation(Prometheus.common)
    testImplementation(Awaitility.awaitility)
    testImplementation(Junit.api)
    testImplementation(Junit.engine)
    testImplementation(KafkaTestContainers.kafka)
    testImplementation(Kotest.assertionsCore)
    testImplementation(Ktor.Test.serverTestHost)
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

val libraryVersion: String = properties["lib_version"]?.toString() ?: "latest-local"

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

            val sourcesJar by tasks.creating(Jar::class) {
                archiveClassifier.set("sources")
                from(sourceSets.main.get().allSource)
            }

            artifact(sourcesJar)
        }
    }
}
