package no.nav.tms.kafka.reader

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.prometheus.client.CollectorRegistry
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import java.net.InetAddress
import java.time.Duration
import java.util.*

class KafkaApplication internal constructor(
    private val ktor: ApplicationEngine,
    private val reader: KafkaReader
) {
    init {
        Runtime.getRuntime().addShutdownHook(Thread(::shutdownHook))
    }

    private val log = KotlinLogging.logger {}

    private val gracePeriod = 5000L
    private val forcefulShutdownTimeout = 30000L

    fun start() {
        ktor.start(wait = false)
        try {
            reader.start()
        } finally {
            log.info { "shutting down ktor, waiting $gracePeriod ms for workers to exit. Forcing shutdown after $forcefulShutdownTimeout ms" }
            ktor.stop(gracePeriod, forcefulShutdownTimeout)
            log.info { "ktor shutdown complete. goodbye." }
        }
    }

    internal fun stop() {
        reader.stop()
        ktor.stop(gracePeriod, forcefulShutdownTimeout)
    }

    fun isRunning() = reader.isRunning()

    private fun shutdownHook() {
        log.info { "received shutdown signal, stopping app" }
        stop()
    }

    companion object {
        fun build(config: KafkaApplicationBuilder.() -> Unit): KafkaApplication {
            return KafkaApplicationBuilder().also(config).build()
        }
    }
}

class KafkaApplicationBuilder internal constructor() {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    init {
        Thread.currentThread().setUncaughtExceptionHandler(::uncaughtExceptionHandler)
    }

    var httpPort: Int = 8080

    private var customizableModule: Application.() -> Unit = { }
    private var startupHook: () -> Unit = { }
    private var shutdownHook: () -> Unit = { }

    private val subscribers: MutableList<Subscriber> = mutableListOf()

    private var collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    private var readerConfig: KafkaReaderConfig? = null

    fun ktorModule(module: Application.() -> Unit) {
        customizableModule = module
    }

    fun subscriber(initializer: () -> Subscriber) {
        subscribers.add(initializer())
    }

    fun onStartup(startupHook: () -> Unit) {
        this.startupHook = startupHook
    }

    fun onShutdown(shutdownHook: () -> Unit) {
        this.shutdownHook = shutdownHook
    }

    fun kafkaConfig(config: KafkaConfigBuilder.() -> Unit) {
        readerConfig = KafkaConfigBuilder()
            .also(config)
            .build()
    }

    private fun uncaughtExceptionHandler(thread: Thread, err: Throwable) {
        log.error(err) { "Uncaught exception in thread ${thread.name}: ${err.message}" }
    }

    internal fun build(): KafkaApplication {
        val config = requireNotNull(readerConfig) { "Kafka configuration must be defined" }

        val reader = KafkaReader(
            factory = ConsumerFactory(config.kafkaConfig),
            groupId = config.consumerGroupId,
            consumerProperties = Properties().apply {
                put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-${config.instanceId}")
                put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, config.instanceId)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetResetConfig)
                put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxRecords)
                put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.maxIntervalMs)
            },
            kafkaTopics = config.kafkaTopics,
            subscribers = subscribers
        )

        return KafkaApplication(
            reader = reader,
            ktor = setupKtorApplication(
                port = httpPort,
                metrics = reader.getMetrics(),
                collectorRegistry = collectorRegistry,
                isAliveCheck = reader::isRunning,
                customizeableModule = customizableModule,
                onStartup = startupHook,
                onShutdown = shutdownHook,
            )
        )
    }
}

class KafkaConfigBuilder internal constructor() {
    private val kafkaTopics: MutableList<String> = mutableListOf()
    fun readTopic(topic: String) = kafkaTopics.add(topic)
    fun readTopics(vararg topics: String) = kafkaTopics.addAll(topics)

    var groupId: String? = null
    var resetPolicy: String = OffsetResetStrategy.EARLIEST.name
    var maxRecords: Int = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS
    var maxInterval: Duration = Duration.ofSeconds(120L + this.maxRecords)

    var environment: Map<String, String> = System.getenv()

    var enableSSL: Boolean = true

    internal fun build(): KafkaReaderConfig {
        require(kafkaTopics.isNotEmpty()) { "Must supply at least 1 kafka topic from which to read" }
        requireNotNull(groupId) { "Must define groupId" }

        return KafkaReaderConfig(
            instanceId = generateInstanceId(environment),
            kafkaTopics = kafkaTopics,
            kafkaConfig = KafkaConfig.fromEnv(enableSSL, environment),
            consumerGroupId = groupId!!,
            autoOffsetResetConfig = resetPolicy.lowercase(),
            maxIntervalMs = maxInterval.toMillis().toInt(),
            maxRecords = maxRecords
        )
    }

    private fun generateInstanceId(env: Map<String, String>): String {
        if (env.containsKey("NAIS_APP_NAME")) return InetAddress.getLocalHost().hostName
        return UUID.randomUUID().toString()
    }
}

internal class KafkaReaderConfig (
    val instanceId: String,
    val kafkaTopics: List<String> = emptyList(),
    val kafkaConfig: KafkaConfig,
    val consumerGroupId: String,
    val autoOffsetResetConfig: String,
    val maxIntervalMs: Int,
    val maxRecords: Int
)
