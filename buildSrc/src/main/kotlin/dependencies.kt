interface DependencyGroup {
    val groupId: String? get() = null
    val version: String? get() = null

    fun dependency(name: String, groupId: String? = this.groupId, version: String? = this.version): String {
        requireNotNull(groupId)
        requireNotNull(version)

        return "$groupId:$name:$version"
    }
}

object Awaitility: DependencyGroup {
    override val groupId get() = "org.awaitility"
    override val version get() = "4.2.2"

    val awaitility get () = dependency("awaitility")
}

object JacksonDatatype: DependencyGroup {
    override val version get() = "2.18.1"

    val datatypeJsr310 get() = dependency("jackson-datatype-jsr310", groupId = "com.fasterxml.jackson.datatype")
    val moduleKotlin get() = dependency("jackson-module-kotlin", groupId = "com.fasterxml.jackson.module")
}

object Junit: DependencyGroup {
    override val groupId get() = "org.junit.jupiter"
    override val version get() = "5.11.3"

    val api get() = dependency("junit-jupiter-api")
    val engine get() = dependency("junit-jupiter-engine")
}

object KafkaTestContainers: DependencyGroup {
    override val groupId get() = "org.testcontainers"
    override val version get() = "1.20.4"

    val kafka get() = dependency("kafka")
}

object Kafka: DependencyGroup {
    override val groupId get() = "org.apache.kafka"
    override val version get() = "3.9.0"

    val kafka_2_12 get() = dependency("kafka_2.12")
}

object Kotest: DependencyGroup {
    override val groupId get() = "io.kotest"
    override val version get() = "5.9.1"

    val assertionsCore get() = dependency("kotest-assertions-core")
    val extensions get() = dependency("kotest-extensions")
}

object Kotlin: DependencyGroup {
    override val groupId get() = "org.jetbrains.kotlin"
    override val version get() = "2.0.21"
}

object KotlinLogging: DependencyGroup {
    override val groupId get() = "io.github.oshai"
    override val version get() = "7.0.0"

    val logging get() = dependency("kotlin-logging")
}

object Kotlinx: DependencyGroup {
    override val groupId get() = "org.jetbrains.kotlinx"

    val coroutines get() = dependency("kotlinx-coroutines-core", version = "1.8.0")
}

object Ktor {
    val version get() = "3.0.1"
    val groupId get() = "io.ktor"

    object Server: DependencyGroup {
        override val groupId get() = Ktor.groupId
        override val version get() = Ktor.version

        val auth get() = dependency("ktor-server-auth")
        val authJwt get() = dependency("ktor-server-auth-jwt")
        val core get() = dependency("ktor-server-core")
        val cio get() = dependency("ktor-server-cio")
        val metricsMicrometer get() = dependency("ktor-server-metrics-micrometer")
    }

    object Test: DependencyGroup {
        override val groupId get() = Ktor.groupId
        override val version get() = Ktor.version

        val serverTestHost get() = dependency("ktor-server-test-host")
    }
}

object Micrometer: DependencyGroup {
    override val groupId get() = "io.micrometer"
    override val version get() = "1.14.1"

    val registryPrometheus get() = dependency("micrometer-registry-prometheus")
}

object Prometheus: DependencyGroup {
    override val version get() = "1.3.4"
    override val groupId get() = "io.prometheus"

    val metricsCore get() = dependency("prometheus-metrics-core")
    val exporterCommon get() = dependency("prometheus-metrics-exporter-common")
}

object TmsKtorTokenSupport: DependencyGroup {
    override val groupId get() = "no.nav.tms.token.support"
    override val version get() = "5.0.0"

    val azureValidation get() = dependency("azure-validation")
    val azureValidationMock get() = dependency("azure-validation-mock")
}

