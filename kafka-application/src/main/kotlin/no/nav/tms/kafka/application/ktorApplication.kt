package no.nav.tms.kafka.application

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
    isAliveCheck: () -> Boolean,
    port: Int = 8080,
    metrics: List<MeterBinder>,
    collectorRegistry: CollectorRegistry,
    customizeableModule: Application.() -> Unit,
    onStartup: () -> Unit,
    onReady: () -> Unit,
    onShutdown: () -> Unit
) = embeddedServer(
    factory = CIO,
    environment = applicationEngineEnvironment {
        connector {
            this.port = port
        }
        module(metaEndpoints(isAliveCheck, collectorRegistry, metrics))

        module(customizeableModule)

        module {
            environment.monitor.subscribe(ApplicationStarted) {
                onStartup()
            }

            environment.monitor.subscribe(ApplicationStarted) {
                onReady()
            }

            environment.monitor.subscribe(ApplicationStopped) {
                onShutdown()
            }
        }
    }
)

private fun metaEndpoints(
    isAliveCheck: () -> Boolean,
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
            if (isAliveCheck()) {
                call.respond(HttpStatusCode.OK)
            } else {
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
