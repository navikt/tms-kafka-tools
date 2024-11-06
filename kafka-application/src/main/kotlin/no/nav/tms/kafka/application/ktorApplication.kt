package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.response.respond
import io.ktor.server.response.respondTextWriter
import io.ktor.server.routing.*
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Metrics.addRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat

private const val isAliveEndpoint = "/isalive"
private const val isReadyEndpoint = "/isready"
private const val metricsEndpoint = "/metrics"

internal fun setupKtorApplication(
    port: Int = 8080,
    metrics: List<MeterBinder>,
    collectorRegistry: CollectorRegistry,
    customizeableModule: Application.() -> Unit,
    readerJob: () -> Unit,
    onStartup: ((Application) -> Unit)?,
    onReady: ((ApplicationEnvironment) -> Unit)?,
    onShutdown: ((Application) -> Unit)?,
    healthChecks: List<HealthCheck>,
    recordBroadcaster: RecordBroadcaster
) = embeddedServer(
    factory = CIO,
    environment = applicationEngineEnvironment {
        connector {
            this.port = port
        }
        module(metaEndpoints(healthChecks, collectorRegistry, metrics))

        module {
            install(MessageChannel) {
                broadcaster = recordBroadcaster
            }
        }

        module {
            customizeableModule()
        }

        module {

            environment.monitor.subscribe(ServerReady) {
                readerJob()
            }

            onStartup?.let {
                environment.monitor.subscribe(ApplicationStarted) {
                    it.runHook("onStartup", onStartup)
                }
            }

            onReady?.let {
                environment.monitor.subscribe(ServerReady) {
                    it.runHook("onReady", onReady)
                }
            }

            onShutdown?.let {
                environment.monitor.subscribe(ApplicationStopped) {
                    it.runHook("onShutdown", onShutdown)
                }
            }
        }
    }
)

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

private fun metaEndpoints(
    healthChecks: List<HealthCheck>,
    collectorRegistry: CollectorRegistry,
    metrics: List<MeterBinder>
): Application.() -> Unit = {
    install(MicrometerMetrics) {
        registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM)
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

        get(metricsEndpoint) {
            val names = call.request.queryParameters.getAll("name[]")?.toSet() ?: emptySet()
            call.respondTextWriter(ContentType.parse(TextFormat.CONTENT_TYPE_004)) {
                TextFormat.write004(this, collectorRegistry.filteredMetricFamilySamples(names))
            }
        }
    }
}
