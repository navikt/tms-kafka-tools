package no.nav.tms.kafka.application

import kotlin.reflect.KClass

class MessageTracker internal constructor() {
    private val subscriberHistory: MutableMap<String, MutableList<TrackerEntry>> = mutableMapOf()

    internal fun track(subscriber: Subscriber, message: JsonMessage, outcome: MessageOutcome) {
        val subscriberName = subscriber::class.simpleName!!

        if (subscriberHistory.contains(subscriberName)) {
            subscriberHistory[subscriberName]!! += TrackerEntry(message, outcome)
        } else {
            subscriberHistory[subscriberName] = mutableListOf(TrackerEntry(message, outcome))
        }
    }

    fun reset() {
        subscriberHistory.clear()
    }

    fun <T : Subscriber> findOutcome(subscriber: KClass<T>, predicate: (JsonMessage) -> Boolean): MessageOutcome? {
        return subscriberHistory[subscriber.simpleName]
            ?.find { predicate(it.message) }
            ?.outcome
    }

    fun <T : Subscriber> findFailedOutcome(subscriber: KClass<T>, predicate: (JsonMessage) -> Boolean): MessageFailed? {
        return subscriberHistory[subscriber.simpleName]
            ?.filter { predicate(it.message) }
            ?.map { it.outcome }
            ?.first { it is MessageFailed }
            ?.let { it as MessageFailed }
    }

    fun <T : Subscriber> findOutcomes(subscriber: KClass<T>): List<Pair<JsonMessage, MessageOutcome>> {
        return subscriberHistory[subscriber.simpleName]
            ?.map { it.message to it.outcome }
            ?: emptyList()
    }

    fun <T : Subscriber> findFailedOutcomes(subscriber: KClass<T>): List<Pair<JsonMessage, MessageFailed>> {
        return subscriberHistory[subscriber.simpleName]
            ?.filter { it.outcome is MessageFailed }
            ?.map { it.message to it.outcome as MessageFailed }
            ?: emptyList()
    }

    fun getStats(): List<TrackerStats> {
        return subscriberHistory.toList().map { (subscriber, history) ->
            collectStats(subscriber, history)
        }
    }

    fun <T : Subscriber> getStats(subscriber: KClass<T>): TrackerStats? {
        return subscriberHistory[subscriber.simpleName]?.let { history ->
            collectStats(subscriber.simpleName!!, history)
        }
    }

    private fun collectStats(subscriber: String, history: List<TrackerEntry>): TrackerStats {
        val count = history
            .groupBy { it.outcome.status }
            .mapValues { it.value.size }

        return TrackerStats(
            subscriber = subscriber,
            accepted = count[MessageStatus.Accepted]?: 0,
            ignored = count[MessageStatus.Ignored]?: 0,
            failed = count[MessageStatus.Failed]?: 0,
        )
    }
}

class TrackerStats(
    val subscriber: String,
    val accepted: Int,
    val ignored: Int,
    val failed: Int
)

private data class TrackerEntry(
    val message: JsonMessage,
    val outcome: MessageOutcome
)
