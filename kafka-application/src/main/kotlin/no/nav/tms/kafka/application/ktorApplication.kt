package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Metrics.addRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.expositionformats.ExpositionFormats
import io.prometheus.metrics.model.registry.MetricNameFilter
import io.prometheus.metrics.model.registry.PrometheusRegistry

private const val isAliveEndpoint = "/isalive"
private const val isReadyEndpoint = "/isready"
private const val metricsEndpoint = "/metrics"

internal fun setupKtorApplication(
    port: Int = 8080,
    metrics: List<MeterBinder>,
    customizeableModule: Application.() -> Unit,
    readerJob: () -> Unit,
    onStartup: ((Application) -> Unit)?,
    onReady: ((ApplicationEnvironment) -> Unit)?,
    onShutdown: ((Application) -> Unit)?,
    healthChecks: List<HealthCheck>,
    recordBroadcaster: RecordBroadcaster
): KtorServer = embeddedServer(
    factory = CIO,
    configure = {
        connector {
            this.port = port
        }
    },
    module = {
        // Setup /isalive, /isready and /metrics
        metaEndpoints(healthChecks, metrics)

        // Setup MessageChannel
        install(MessageChannel) {
            broadcaster = recordBroadcaster
        }

        // Apply user-defined module
        customizeableModule()

        // Setup lifecycle hooks
        monitor.subscribe(ServerReady) {
            readerJob()
        }

        onStartup?.let {
            monitor.subscribe(ApplicationStarted) {
                it.runHook("onStartup", onStartup)
            }
        }

        onReady?.let {
            monitor.subscribe(ServerReady) {
                it.runHook("onReady", onReady)
            }
        }

        onShutdown?.let {
            monitor.subscribe(ApplicationStopped) {
                it.runHook("onShutdown", onShutdown)
            }
        }
    }
)

internal typealias KtorServer = EmbeddedServer<out ApplicationEngine, out ApplicationEngine.Configuration>

private val logger = KotlinLogging.logger {}
private val secureLog = KotlinLogging.logger("secureLog")

private fun <T> T.runHook(eventHook: String, block: (T) -> Unit) {
    logger.info { "Executing user-defined hook '$eventHook'" }
    try {
        block(this)
    } catch (e: Exception) {
        logger.error { "Encountered error while executing user-defined event hook '$eventHook'" }
        secureLog.error(e) { "Encountered error while executing user-defined event hook '$eventHook'" }
    }
}

private fun Application.metaEndpoints(
    healthChecks: List<HealthCheck>,
    metrics: List<MeterBinder>
) {

    install(MicrometerMetrics) {
        registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)
        meterBinders = meterBinders + metrics
        addRegistry(registry)
    }

    routing {
        get(isAliveEndpoint) {
            val failingTests = healthChecks.filter { it.checkFunction() == AppHealth.Unhealthy }

            if (failingTests.isEmpty()) {
                call.respond(HttpStatusCode.OK)
            } else {
                val names = failingTests.map { "'${it.name}'" }
                logger.info { "Application is unhealthy due to failing health checks: $names" }
                call.respond(HttpStatusCode.ServiceUnavailable)
            }
        }

        get(isReadyEndpoint) {
            call.respond(HttpStatusCode.OK)
        }

        val writer = ExpositionFormats.init().openMetricsTextFormatWriter

        get(metricsEndpoint) {
            PrometheusRegistry.defaultRegistry.scrape()

            val requestedNames = call.request.queryParameters.getAll("name[]")?.toSet() ?: emptySet()

            val filter = if (requestedNames.isNotEmpty()) {
                MetricNameFilter.builder().nameMustBeEqualTo(requestedNames).build()
            } else {
                null
            }

            call.respondOutputStream(ContentType.parse(writer.contentType)) {
                writer.write(this, PrometheusRegistry.defaultRegistry.scrape(filter))
            }
        }
    }
}
