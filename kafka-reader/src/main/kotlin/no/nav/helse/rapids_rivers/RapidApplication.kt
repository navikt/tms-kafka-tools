package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.prometheus.client.CollectorRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import java.net.InetAddress
import java.time.Duration
import java.util.*

class RapidApplication internal constructor(
    private val ktor: ApplicationEngine,
    private val rapid: RapidsConnection,
    private val onKtorStartup: () -> Unit = {},
    private val onKtorShutdown: () -> Unit = {}
) : RapidsConnection() {

    init {
        Runtime.getRuntime().addShutdownHook(Thread(::shutdownHook))
    }

    override fun start() {
        ktor.start(wait = false)
        try {
            onKtorStartup()
            rapid.start()
        } finally {
            onKtorShutdown()
            val gracePeriod = 5000L
            val forcefulShutdownTimeout = 30000L
            log.info { "shutting down ktor, waiting $gracePeriod ms for workers to exit. Forcing shutdown after $forcefulShutdownTimeout ms" }
            ktor.stop(gracePeriod, forcefulShutdownTimeout)
            log.info { "ktor shutdown complete: end of life. goodbye." }
        }
    }

    override fun stop() {
        rapid.stop()
    }

    private fun shutdownHook() {
        log.info { "received shutdown signal, stopping app" }
        stop()
    }

    companion object {
        private val log = KotlinLogging.logger {}

        fun create(env: Map<String, String>, configure: (ApplicationEngine, KafkaRapid) -> Unit = {_, _ -> }) =
            Builder(RapidApplicationConfig.fromEnv(env))
                .build(configure)
    }

    class Builder(private val config: RapidApplicationConfig) {

        init {
            Thread.currentThread().setUncaughtExceptionHandler(::uncaughtExceptionHandler)
        }

        private val rapid = KafkaRapid(
            factory = ConsumerProducerFactory(config.kafkaConfig),
            groupId = config.consumerGroupId,
            consumerProperties = Properties().apply {
                put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-${config.instanceId}")
                put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, config.instanceId)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetResetConfig?.lowercase() ?: OffsetResetStrategy.LATEST.name.lowercase())
                put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "${config.maxRecords}")
                put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "${config.maxIntervalMs}")
            },
            kafkaTopics = config.kafkaTopics,
        )

        private var ktor: ApplicationEngine? = null
        private val modules = mutableListOf<Application.() -> Unit>()

        fun withKtor(ktor: ApplicationEngine) = apply {
            this.ktor = ktor
        }

        fun withKtorModule(module: Application.() -> Unit) = apply {
            this.modules.add(module)
        }

        fun build(configure: (ApplicationEngine, KafkaRapid) -> Unit = { _, _ -> }, configuration: NettyApplicationEngine.Configuration.() -> Unit = { } ): RapidsConnection {
            val app = ktor ?: defaultKtorApp(configuration)
            configure(app, rapid)
            return RapidApplication(app, rapid)
        }

        private fun defaultKtorApp(configuration: NettyApplicationEngine.Configuration.() -> Unit): ApplicationEngine {
            return defaultNaisApplication(
                port = config.httpPort,
                extraMetrics = rapid.getMetrics(),
                collectorRegistry = config.collectorRegistry,
                isAliveCheck = rapid::isRunning,
                extraModules = modules,
                configuration = configuration
            )
        }

        private fun uncaughtExceptionHandler(thread: Thread, err: Throwable) {
            log.error(err) { "Uncaught exception in thread ${thread.name}: ${err.message}" }
        }
    }

    class RapidApplicationConfig(
        internal val appName: String?,
        internal val instanceId: String,
        internal val kafkaTopics: List<String> = emptyList(),
        internal val kafkaConfig: KafkaConfig,
        internal val consumerGroupId: String,
        internal val autoOffsetResetConfig: String? = null,
        internal val autoCommit: Boolean? = false,
        maxIntervalMs: Long? = null,
        maxRecords: Int? = null,
        internal val httpPort: Int = 8080,
        internal val collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry
    ) {
        internal val maxRecords = maxRecords ?: ConsumerConfig.DEFAULT_MAX_POLL_RECORDS
        // assuming a "worst case" scenario where it takes 4 seconds to process each message;
        // then set MAX_POLL_INTERVAL_MS_CONFIG 2 minutes above this "worst case" limit so
        // the broker doesn't think we have died (and revokes partitions)
        internal val maxIntervalMs: Long = maxIntervalMs ?: Duration.ofSeconds(120 + this.maxRecords * 4.toLong()).toMillis()

        companion object {
            fun fromEnv(env: Map<String, String>, kafkaConfig: KafkaConfig = KafkaConfig.default) = RapidApplicationConfig(
                appName = env["RAPID_APP_NAME"] ?: generateAppName(env) ?: log.info { "app name not configured" }.let { null },
                instanceId = generateInstanceId(env),
                kafkaTopics = env["KAFKA_EXTRA_TOPIC"]?.split(',')?.map(String::trim) ?: emptyList(),
                kafkaConfig = kafkaConfig,
                consumerGroupId = env.getValue("KAFKA_CONSUMER_GROUP_ID"),
                autoOffsetResetConfig = env["KAFKA_RESET_POLICY"],
                autoCommit = env["KAFKA_AUTO_COMMIT"]?.toBoolean(),
                maxIntervalMs = env["KAFKA_MAX_POLL_INTERVAL_MS"]?.toLong(),
                maxRecords = env["KAFKA_MAX_RECORDS"]?.toInt() ?: ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                httpPort =  env["HTTP_PORT"]?.toInt() ?: 8080
            )

            private fun generateInstanceId(env: Map<String, String>): String {
                if (env.containsKey("NAIS_APP_NAME")) return InetAddress.getLocalHost().hostName
                return UUID.randomUUID().toString()
            }

            private fun generateAppName(env: Map<String, String>): String? {
                val appName = env["NAIS_APP_NAME"] ?: return log.info { "not generating app name because NAIS_APP_NAME not set" }.let { null }
                val namespace = env["NAIS_NAMESPACE"] ?: return log.info { "not generating app name because NAIS_NAMESPACE not set" }.let { null }
                val cluster = env["NAIS_CLUSTER_NAME"] ?: return log.info { "not generating app name because NAIS_CLUSTER_NAME not set" }.let { null }
                return "$appName-$cluster-$namespace"
            }
        }
    }
}
