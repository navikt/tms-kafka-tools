package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.netty.*
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

fun defaultNaisApplication(
    isAliveCheck: () -> Boolean,
    port: Int = 8080,
    extraMetrics: List<MeterBinder> = emptyList(),
    collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry,
    extraModules: List<Application.() -> Unit> = emptyList(),
    configuration: NettyApplicationEngine.Configuration.() -> Unit = { },
) = embeddedServer(Netty, applicationEngineEnvironment {
    connectors.add(EngineConnectorBuilder().apply {
        this.port = port
    })
    module(metaEndpoints(isAliveCheck, collectorRegistry, extraMetrics))
    modules.addAll(extraModules)
}) {
    apply(configuration)
    KotlinLogging.logger {}
}

private fun metaEndpoints(
    isAliveCheck: () -> Boolean,
    collectorRegistry: CollectorRegistry,
    metrics: List<MeterBinder>
) = fun Application.() {
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
