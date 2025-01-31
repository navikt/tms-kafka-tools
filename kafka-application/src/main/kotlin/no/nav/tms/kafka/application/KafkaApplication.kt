package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import java.net.InetAddress
import java.util.*

class KafkaApplication internal constructor(
    private val ktor: KtorServer,
    private val reader: KafkaReader
) {
    init {
        Runtime.getRuntime().addShutdownHook(Thread(::shutdownHook))
    }

    private val log = KotlinLogging.logger {}

    private val gracePeriod = 5000L
    private val forcefulShutdownTimeout = 30000L

    fun start() {
        ktor.start(wait = true)
    }

    fun stop() {
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
    private var startupHook: ((Application) -> Unit)? = null
    private var readyHook: ((ApplicationEnvironment) -> Unit)? = null
    private var shutdownHook: ((Application) -> Unit)? = null

    private val healthChecks: MutableList<HealthCheck> = mutableListOf()

    private val subscribers: MutableList<Subscriber> = mutableListOf()

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

    fun onStartup(startupHook: (Application) -> Unit) {
        this.startupHook = startupHook
    }

    fun onReady(readyHook: (ApplicationEnvironment) -> Unit) {
        this.readyHook = readyHook
    }

    fun onShutdown(shutdownHook: (Application) -> Unit) {
        this.shutdownHook = shutdownHook
    }

    fun kafkaConfig(config: KafkaReaderConfigBuilder.() -> Unit) {
        readerConfig = KafkaReaderConfigBuilder()
            .also(config)
            .build()
    }

    fun healthCheck(name: String? = null, checkFunction: () -> AppHealth) {
        healthChecks.add(HealthCheck.create(name, checkFunction))
    }

    private fun uncaughtExceptionHandler(thread: Thread, err: Throwable) {
        log.error(err) { "Uncaught exception in thread ${thread.name}: ${err.message}" }
    }

    internal fun build(): KafkaApplication {

        val config = requireNotNull(readerConfig) { "Kafka configuration must be defined" }

        val broadcaster = RecordBroadcaster(subscribers, config.eventNameFields)

        val reader = KafkaReader(
            factory = ConsumerFactory.init(
                clientId = config.clientId,
                enableSsl = config.enableSsl,
                env = config.environment,
                properties = config.properties
            ),
            groupId = config.groupId,
            kafkaTopics = config.kafkaTopics,
            broadcaster = broadcaster
        )

        val readerHealthCheck = HealthCheck.create("Kafka reader is running") {
            if (reader.isRunning()) {
                AppHealth.Healthy
            } else {
                AppHealth.Unhealthy
            }
        }

        healthChecks.add(readerHealthCheck)

        return KafkaApplication(
            reader = reader,
            ktor = setupKtorApplication(
                port = httpPort,
                metrics = reader.getMetrics(),
                customizeableModule = customizableModule,
                readerJob = { reader.start() },
                onStartup = startupHook,
                onShutdown = shutdownHook,
                onReady = readyHook,
                healthChecks = healthChecks,
                recordBroadcaster = broadcaster
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

internal class HealthCheck(
    val name: String,
    val checkFunction: () -> AppHealth
) {
    companion object {
        private var healthCheckId = 1

        fun create(name: String?, checkFunction: () -> AppHealth): HealthCheck {
            val checkName = name ?: "Unnamed healthcheck ${healthCheckId++}"

            return HealthCheck(checkName, checkFunction)
        }
    }
}

enum class AppHealth {
    Healthy, Unhealthy
}
