package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging

abstract class Subscriber {
    internal fun name() = this::class.simpleName

    private val log = KotlinLogging.logger {}

    private val subscription by lazy { subscribe() }

    abstract fun subscribe(): Subscription
    abstract suspend fun receive(jsonMessage: JsonMessage)

    suspend fun onMessage(jsonMessage: JsonMessage) {
        val message = jsonMessage.withFields(subscription.knownFields)

        subscription.tryAccept(message, ::receive) {
            log.debug { "Subscriber [${this::class.simpleName}] rejected message ${jsonMessage.json} due to [${it.explainReason()}]." }
        }
    }
}

internal data class IgnoreReason(
    val ignoredEvent: String?,
    val missingFields: List<String>,
    val missingValues: Map<String, List<Any>>,
    val unwantedValues: Map<String, List<Any>>
) {
    fun explainReason(): String {
        return when {
            ignoredEvent != null -> "not listening for event \"$ignoredEvent\""
            else -> {
                listOf(
                    if (missingFields.isNotEmpty()) "missing required fields [${missingFields.joinToString()}}]" else "",
                    if (missingValues.isNotEmpty()) "missing required values [${missingValues.describe()}}]" else "",
                    if (unwantedValues.isNotEmpty()) "contains unwanted values [${unwantedValues.describe()}}]" else "",
                )
                    .filter { it.isNotEmpty() }
                    .joinToString()
            }
        }
    }

    private fun Map<String, List<Any>>.describe() = map {(field, values) ->
        "\"$field\": ${values.joinToString(prefix = "[\"", postfix = "\"]", separator = "\", \"")}"
    }

    companion object {
        fun incorrectEvent(name: String) = IgnoreReason(
            ignoredEvent = name,
            missingFields = emptyList(),
            missingValues = emptyMap(),
            unwantedValues = emptyMap()
        )
    }
}

class Subscription private constructor(private val eventName: String) {

    internal val knownFields = mutableSetOf<String>()

    private val requiredFields: MutableSet<String> = mutableSetOf()
    private val optionalFields: MutableSet<String> = mutableSetOf()
    private val requiredValues: MutableMap<String, List<Any>> = mutableMapOf()
    private val rejectedValues: MutableMap<String, List<Any>> = mutableMapOf()

    fun withFields(vararg fields: String) = also {
        knownFields.addAll(fields)
        requiredFields.addAll(fields)
    }

    fun withOptionalFields(vararg fields: String) = also {
        knownFields.addAll(fields)
        optionalFields.addAll(fields)
    }

    fun withValue(field: String, value: Any) = also {
        if (!value.isPrimitive()) {
            throw SubscriptionException(
                "Tried to require field \"$field\" to be value of type ${value::class.simpleName}. Required value must be primitive."
            )
        }

        knownFields.add(field)
        requiredValues += field to listOf(value)
    }

    fun withAnyValue(field: String, vararg values: Any) = also {
        if (values.any { !it.isPrimitive() }) {
            throw SubscriptionException(
                "Tried to require field \"$field\" to be a value of incompatible type(s). Required value(s) must be primitive."
            )
        }

        knownFields.add(field)
        requiredValues += field to values.asList()
    }

    fun rejectValue(field: String, value: Any) = also {
        if (!value.isPrimitive()) {
            throw SubscriptionException(
                "Tried to require field \"$field\" not to be value of type ${value::class.simpleName}. Required value must be primitive."
            )
        }

        knownFields.add(field)
        rejectedValues += field to listOf(value)
    }

    fun rejectValues(field: String, vararg values: Any) = also {
        if (values.any { !it.isPrimitive() }) {
            throw SubscriptionException(
                "Tried to require field \"$field\" not to be a value of incompatible type(s). Required value(s) must be primitive."
            )
        }
        knownFields.add(field)
        rejectedValues += field to values.asList()
    }

    private fun Any.isPrimitive() = when (this) {
        is Number, is Boolean, is String -> true
        else -> false
    }

    internal suspend fun tryAccept(
        jsonMessage: JsonMessage,
        onAccept: suspend (JsonMessage) -> Unit,
        onIgnore: (IgnoreReason) -> Unit
    ) {
        if (jsonMessage.eventName != eventName) {
            onIgnore(IgnoreReason.incorrectEvent(jsonMessage.eventName))
            return
        }

        val presentFields = jsonMessage.json.fields().asSequence().toList().map { it.key }.toSet()

        val missingFields = (requiredFields - presentFields).toList()

        val missingValues = requiredValues.filter { (field, values) ->
            values.none { value ->
                valueIsPresent(jsonMessage, field, value)
            }
        }

        val unwantedValues = rejectedValues.filter { (field, values) ->
            values.any { value ->
                valueIsPresent(jsonMessage, field, value)
            }
        }

        if (missingFields.isEmpty() && missingValues.isEmpty() && unwantedValues.isEmpty()) {
            onAccept(jsonMessage)
        } else {
            onIgnore(
                IgnoreReason(
                    ignoredEvent = null,
                    missingFields = missingFields,
                    missingValues = missingValues,
                    unwantedValues = unwantedValues
                )
            )
        }
    }

    private fun valueIsPresent(jsonMessage: JsonMessage, field: String, value: Any): Boolean {
        val node = jsonMessage.json[field]

        if (node.isMissingOrNull() || node.isContainerNode) {
            //todo more context?
            return false
        }

        return when (value) {
            is Int -> node.isInt && node.asInt() == value
            is Long -> node.isLong && node.asLong() == value
            is Float -> node.isFloat && node.asDouble().toFloat() == value
            is Double -> node.isDouble && node.asDouble() == value
            is Boolean -> node.isBoolean && node.asBoolean() == value
            is String -> node.isTextual && node.asText() == value
            else -> throw IllegalStateException("Unknown type in required values: ${value::class.simpleName}")
        }
    }

    companion object {
        fun forEvent(name: String) = Subscription(name)
    }
}

class SubscriptionException(message: String): IllegalArgumentException(message)
