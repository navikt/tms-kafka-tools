package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging
import org.slf4j.LoggerFactory

interface MessageContext

abstract class RapidsConnection : MessageContext {
    private companion object {
        private val log = KotlinLogging.logger {}
        private val secureLog = KotlinLogging.logger("secureLog")
    }

    private val listeners = mutableListOf<MessageListener>()

    fun register(listener: MessageListener) {
        listeners.add(listener)
    }

    protected fun notifyMessage(message: String, context: MessageContext) {
        listeners.forEach { it.onMessage(message, context) }
    }

    abstract fun start()
    abstract fun stop()

    fun interface MessageListener {
        fun onMessage(message: String, context: MessageContext)
    }
}
