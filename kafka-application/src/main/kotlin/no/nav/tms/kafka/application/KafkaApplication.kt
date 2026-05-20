package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.application.*
import java.net.InetAddress
import java.util.*

class KafkaApplication internal constructor(
    private val ktor: KtorServer,
    private val reader: KafkaReader,
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

    private var mdcConfigured = false
    private var mdcConfig: MinSideMdcConfig? = null

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

    /**
     * Configure MDC-fields [domain,event, produced_by og minside_id] for messages from kafka based.
     *   @param enabled set to false to disable MDC configuration.
     *   @param idFieldName  name of the  field in JsonMessage som that contains the uniqe identifier for the message. This field must be of type String and not be blank.
     *   @param producedByFieldName name of the field in JsonMessage that contains the name of the producer of the message. This field must be of type String and not be blank.
     *   @param domain domain of the message. Predefined domains are "utkast", "varsel" and "microfrontend". Custom domains can be created with [Domain.custom(name)] but must not contain the predefined domain names.Must be 4-15 characters and can only contain lowercase letters and hyphens.
     *   @param allowMissingProducerField set to true to allow messages without a producer field. Default is false.
     */
    fun minSideMdc(configBuilder: MinSideMdcConfigBuilder.() -> Unit) {
        if (this.mdcConfigured) {
            throw MinSideMdcConfigException("Kan ikke configurere minside-mdc flere ganger")
        }

        val mdcConfigBuilder = MinSideMdcConfigBuilder()
            .apply { configBuilder() }
            .apply { validate() }

        if (mdcConfigBuilder.enabled) {
            this.mdcConfig = mdcConfigBuilder.build()
        } else {
            this.mdcConfig = null
        }

        this.mdcConfigured = true
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
        if (!mdcConfigured) {
            throw IllegalStateException("MinSideMDC must be configured or disabled")
        }
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

        mdcConfig?.let {
            log.info { "Setter opp MinSideMDC ${it.description}" }
            subscribers.forEach { subscriber ->
                subscriber.configureMinSideMdc(it)
            }
        } ?: run {
            log.info { "Setter ikke opp MinSideMDC opp i applikasjonen" }
        }


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

        val nameFields = eventNameFields.ifEmpty {
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

class MinSideMdcConfigBuilder internal constructor(
    var enabled: Boolean = true,
    var domain: Domain? = null,
    var idFieldName: String? = null,
    private var _idSupplier: ((JsonMessage) -> String)? = null,
    var producedByFieldName: String? = null,
    private var _producedBySupplier: ((JsonMessage) -> String?)? = null
) {
    fun idSupplier(supplier: (JsonMessage) -> String) {
        _idSupplier = supplier
    }

    fun producedBySupplier(supplier: (JsonMessage) -> String?) {
        _producedBySupplier = supplier
    }

    internal fun validate() {
        if (!enabled) {
            return
        }

        val invalidIdField = (idFieldName.isNullOrBlank() && _idSupplier == null)
            || (!idFieldName.isNullOrBlank() && _idSupplier != null)

        if (invalidIdField) {
            throw MinSideMdcConfigException("Må velge presist én av enkel eller egendefinert måte å hente id fra JsonMessage.")
        }

        val invalidProducedByField = (producedByFieldName == null && _producedBySupplier == null)
            || (producedByFieldName != null && _producedBySupplier != null)

        if (invalidProducedByField) {
            throw MinSideMdcConfigException("Må velge presist én av enkel eller egendefinert måte å hente produsent fra JsonMessage.")
        }

        if (domain == null) {
            throw MinSideMdcConfigException(
                "Må definere domene for applikasjon"
            )
        }
    }

    private fun describe(): String {
        val domainPart = "domain=${domain!!.name}"

        val idFieldPart = if (idFieldName != null) {
            "idFieldName=$idFieldName"
        } else {
            "id-value supplied by user-defined function"
        }

        val producedByFieldPart = if (producedByFieldName != null) {
            "producedByFieldName=$producedByFieldName"
        } else {
            "producedBy-value supplied by user-defined function"
        }

        return "[$domainPart, $idFieldPart, $producedByFieldPart]"
    }

    internal fun build() = MinSideMdcConfig(
        idSupplier = _idSupplier ?: { message -> message[idFieldName!!].asText() },
        producedBySupplier = _producedBySupplier ?: { message -> message[producedByFieldName!!].asText() },
        domain = domain!!,
        description = describe()
    )
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
