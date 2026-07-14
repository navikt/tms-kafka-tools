interface DependencyGroup {
    val groupId: String? get() = null
    val version: String? get() = null

    fun dependency(name: String, groupId: String? = this.groupId, version: String? = this.version): String {
        requireNotNull(groupId)
        requireNotNull(version)

        return "$groupId:$name:$version"
    }
}

object Avro: DependencyGroup {
    override val groupId get() = "io.confluent"
    override val version get() = "8.3.0"

    val avroSerializer get() = dependency("kafka-avro-serializer")
}

object Awaitility: DependencyGroup {
    override val groupId get() = "org.awaitility"
    override val version get() = "4.3.0"

    val awaitility get () = dependency("awaitility")
}

object JacksonDatatype: DependencyGroup {
    override val version get() = "2.21.2"

    val datatypeJsr310 get() = dependency("jackson-datatype-jsr310", groupId = "com.fasterxml.jackson.datatype")
    val moduleKotlin get() = dependency("jackson-module-kotlin", groupId = "com.fasterxml.jackson.module")
}

object JunitJupiter: DependencyGroup {
    override val groupId get() = "org.junit.jupiter"
    override val version get() = "6.0.3"

    val api get() = dependency("junit-jupiter-api")
    val engine get() = dependency("junit-jupiter-engine")
}

object JunitPlatform: DependencyGroup {
    override val groupId get() = "org.junit.platform"
    override val version get() = "6.0.3"

    val launcher get() = dependency("junit-platform-launcher")
}

object Kafka: DependencyGroup {
    override val groupId get() = "org.apache.kafka"
    override val version get() = "4.2.0"

    val clients get() = dependency("kafka-clients")
}

object KafkaTestContainers: DependencyGroup {
    override val groupId get() = "org.testcontainers"
    override val version get() = "2.0.4"

    val kafka get() = dependency("testcontainers-kafka")
}

object Kotest: DependencyGroup {
    override val groupId get() = "io.kotest"
    override val version get() = "6.1.11"

    val assertionsCore get() = dependency("kotest-assertions-core")
    val extensions get() = dependency("kotest-extensions")
}

object Kotlin: DependencyGroup {
    override val groupId get() = "org.jetbrains.kotlin"
    override val version get() = "2.3.20"
}

object KotlinLogging: DependencyGroup {
    override val groupId get() = "io.github.oshai"
    override val version get() = "8.0.01"

    val logging get() = dependency("kotlin-logging")
}

object Kotlinx: DependencyGroup {
    override val groupId get() = "org.jetbrains.kotlinx"

    val coroutines get() = dependency("kotlinx-coroutines-core", version = "1.10.2")
}

object Ktor {
    val version get() = "3.4.2"
    val groupId get() = "io.ktor"

    object Server: DependencyGroup {
        override val groupId get() = Ktor.groupId
        override val version get() = Ktor.version

        val core get() = dependency("ktor-server-core")
        val cio get() = dependency("ktor-server-cio")
        val metricsMicrometer get() = dependency("ktor-server-metrics-micrometer")
        val authJwt get() = dependency("ktor-server-auth-jwt")
    }

    object Test: DependencyGroup {
        override val groupId get() = Ktor.groupId
        override val version get() = Ktor.version

        val serverTestHost get() = dependency("ktor-server-test-host")
    }
}

object Logback: DependencyGroup {
    override val version = "1.5.32"
    val classic = "ch.qos.logback:logback-classic:$version"
}

object Logstash: DependencyGroup {
    override val groupId get() = "net.logstash.logback"
    override val version get() = "9.0"

    val logbackEncoder get() = dependency("logstash-logback-encoder")
}

object Micrometer: DependencyGroup {
    override val groupId get() = "io.micrometer"
    override val version get() = "1.16.5"

    val registryPrometheus get() = dependency("micrometer-registry-prometheus")
}

object Mockk: DependencyGroup {
    override val groupId get() = "io.mockk"
    override val version get() = "1.14.9"

    val mockk get() = dependency("mockk")
}

object Prometheus: DependencyGroup {
    override val version get() = "1.5.1"
    override val groupId get() = "io.prometheus"

    val metricsCore get() = dependency("prometheus-metrics-core")
    val exporterCommon get() = dependency("prometheus-metrics-exporter-common")
}

object TmsKtorTokenSupport: DependencyGroup {
    override val groupId get() = "no.nav.tms.token.support"
    override val version get() = "6.0.0"

    val azureValidation get() = dependency("entra-id-token-verification")
    val azureValidationMock get() = dependency("entra-id-token-verification-mock")
}

object TmsCommonLib: DependencyGroup {
    override val groupId get() = "no.nav.tms.common"
    override val version get() = "5.3.2"

    val teamLogger get() = dependency("team-logger")
}

