import org.gradle.api.tasks.testing.logging.TestExceptionFormat


plugins {
    kotlin("jvm").version(Kotlin.version)
    kotlin("plugin.serialization") version Kotlin.version
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation(JacksonDatatype.datatypeJsr310)
    implementation(JacksonDatatype.moduleKotlin)
    implementation(Micrometer.registryPrometheus)
    implementation(Kotlinx.coroutines)
    implementation(KotlinLogging.logging)
    implementation(Kafka.kafka_2_12)
    implementation(Ktor.Server.core)
    implementation(Ktor.Server.netty)
    implementation(Ktor.Server.metricsMicrometer)
    implementation(Prometheus.simpleClient)
    implementation(Prometheus.common)
    implementation(JacksonDatatype.datatypeJsr310)
    testImplementation(Junit.api)
    testImplementation(Junit.engine)
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
            url = uri("https://maven.pkg.github.com/navikt/tms-varsel-authority")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

    publications {
        create<MavenPublication>("gpr") {
            groupId = "no.nav.tms.varsel"
            artifactId = "java-builder"
            version = libraryVersion
            from(components["java"])
        }
    }
}
