package no.nav.tms.kafka.application

import org.slf4j.MDC
import java.io.Closeable

suspend fun <R> withMDC(context: Map<String, String>, block: suspend () -> R): R {
    return CloseableMDCContext(context).use {
        block()
    }
}

private class CloseableMDCContext(newContext: Map<String, String>) : Closeable {
    private val originalContextMap = MDC.getCopyOfContextMap() ?: emptyMap()

    init {
        MDC.setContextMap(originalContextMap + newContext)
    }

    override fun close() {
        MDC.setContextMap(originalContextMap)
    }
}
