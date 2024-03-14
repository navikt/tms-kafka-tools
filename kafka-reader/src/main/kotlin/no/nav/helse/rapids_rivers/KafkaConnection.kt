package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging

abstract class KafkaConnection {
    private companion object {
        private val log = KotlinLogging.logger {}
        private val secureLog = KotlinLogging.logger("secureLog")
    }

    private val subscribers = mutableListOf<Subscriber>()

    internal fun register(subscriber: Subscriber) {
        subscribers.add(subscriber)
    }

    internal fun notifyMessage(newJsonMessage: NewJsonMessage) {
        subscribers.forEach { it.onMessage(newJsonMessage) }
    }

    abstract fun start()
    abstract fun stop()
}
