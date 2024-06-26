package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.prometheus.client.CollectorRegistry
import java.net.InetAddress
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

    fun subscribers(vararg subscriber: Subscriber) {
        subscribers.addAll(subscriber)
    }

    fun onStartup(startupHook: () -> Unit) {
        this.startupHook = startupHook
    }

    fun onShutdown(shutdownHook: () -> Unit) {
        this.shutdownHook = shutdownHook
    }

    fun kafkaConfig(config: KafkaReaderConfigBuilder.() -> Unit) {
        readerConfig = KafkaReaderConfigBuilder()
            .also(config)
            .build()
    }

    private fun uncaughtExceptionHandler(thread: Thread, err: Throwable) {
        log.error(err) { "Uncaught exception in thread ${thread.name}: ${err.message}" }
    }

    internal fun build(): KafkaApplication {
        val config = requireNotNull(readerConfig) { "Kafka configuration must be defined" }

        val reader = KafkaReader(
            factory = ConsumerFactory.init(
                clientId = config.clientId,
                enableSsl = config.enableSsl,
                env = config.environment,
                properties = config.properties
            ),
            groupId = config.groupId,
            kafkaTopics = config.kafkaTopics,
            broadcaster = RecordBroadcaster(subscribers, config.eventNameFields)
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

class KafkaReaderConfigBuilder internal constructor() {
    private val kafkaTopics: MutableList<String> = mutableListOf()
    fun readTopic(topic: String) = kafkaTopics.add(topic)
    fun readTopics(vararg topics: String) = kafkaTopics.addAll(topics)
    fun withProperties(config: Properties.() -> Unit) = properties.apply(config)

    fun eventNameFields(vararg fieldNames: String) {
        eventNameFields.clear()
        eventNameFields.addAll(fieldNames)
    }

    @Deprecated("Use function eventNameFields()", replaceWith = ReplaceWith("this.eventNameFields()"))
    var eventName: String? = null

    var groupId: String? = null
    var enableSSL: Boolean = true
    var environment: Map<String, String> = System.getenv()

    private val properties = Properties()
    private val eventNameFields = mutableListOf<String>()

    internal fun build(): KafkaReaderConfig {
        require(kafkaTopics.isNotEmpty()) { "Must supply at least 1 kafka topic from which to read" }
        requireNotNull(groupId) { "Must define groupId" }

        val nameFields = if (eventNameFields.isNotEmpty()) {
            eventNameFields
        } else if (eventName != null) {
            listOf(eventName!!)
        } else {
            listOf(JsonMessage.DEFAULT_EVENT_NAME)
        }

        return KafkaReaderConfig(
            clientId = generateClientId(environment),
            kafkaTopics = kafkaTopics,
            groupId = groupId!!,
            enableSsl = enableSSL,
            environment = environment,
            properties = properties,
            eventNameFields = nameFields
        )
    }

    private fun generateClientId(env: Map<String, String>): String {
        return if (env.containsKey("NAIS_APP_NAME")) {
            InetAddress.getLocalHost().hostName
        } else {
            UUID.randomUUID().toString()
        }
    }
}

internal class KafkaReaderConfig(
    val clientId: String,
    val groupId: String,
    val kafkaTopics: List<String> = emptyList(),
    val enableSsl: Boolean,
    val environment: Map<String, String>,
    val properties: Properties,
    val eventNameFields: List<String>
)
