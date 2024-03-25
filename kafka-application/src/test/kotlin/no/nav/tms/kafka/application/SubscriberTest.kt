package no.nav.tms.kafka.application

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.*


class SubscriberTest {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `accepts only messages with correct name`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "sale" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "delivery" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with required fields`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withFields("item")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "item": "apples" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "item": "apples", "price": 25.50 }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2

        """{ "@event_name": "order", "price": 25.50 }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }

    @Test
    fun `accepts only messages with required value`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withValue("color", "green")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts only messages with any of required values`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withAnyValue("color", "green", "red")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }

    @Test
    fun `allows required value to be non-string`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withValue("amount", 10)
                .withValue("price", 10.50)
                .withValue("delivered", true)

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "amount": 5, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 20.0, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": false }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `accepts messages with optional field`() = runBlocking <Unit> {
        val orderListener = object : Subscriber() {
            var deliveries = 0
            var amount = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withOptionalFields("delivered", "amount")

            override suspend fun receive(jsonMessage: JsonMessage) {
                jsonMessage.getOrNull("delivered")?.let {
                    if (it.asBoolean()) {
                        deliveries++

                        amount += jsonMessage.get("amount").asInt()
                    }
                }
            }
        }

        """{ "@event_name": "order", "price": 10.50 }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.deliveries shouldBe 0
        orderListener.amount shouldBe 0

        """{ "@event_name": "order", "price": 20.0, "delivered": false }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.deliveries shouldBe 0
        orderListener.amount shouldBe 0

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": true }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.deliveries shouldBe 1
        orderListener.amount shouldBe 10

        """{ "@event_name": "order", "amount": 10, "price": 10.50, "delivered": false }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.deliveries shouldBe 1
        orderListener.amount shouldBe 10
    }

    @Test
    fun `ignores rejected value`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withoutValue("color", "green")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 2
    }


    @Test
    fun `ignores rejected values`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var count = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withoutValues("color", "blue", "red")

            override suspend fun receive(jsonMessage: JsonMessage) {
                count++
            }
        }

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 0

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.count shouldBe 1
    }

    @Test
    fun `allows custom validation`() = runBlocking<Unit> {
        val primeNumberCounter = object : Subscriber() {
            var primes = 0

            override fun subscribe() = Subscription.forEvent("count")
                .withValidation("number") { it.asInt().isPrime() }

            override suspend fun receive(jsonMessage: JsonMessage) {
                primes++
            }
        }

        (1..10).forEach {
            """{ "@event_name": "count", "number": $it }"""
                .asMessage()
                .let { primeNumberCounter.onMessage(it) }
        }

        primeNumberCounter.primes shouldBe 4 // 2, 3, 5, 7..

        (11..20).forEach {
            """{ "@event_name": "count", "number": $it }"""
                .asMessage()
                .let { primeNumberCounter.onMessage(it) }
        }

        primeNumberCounter.primes shouldBe 8 // ..11, 13, 17, 19..

        (21..30).forEach {
            """{ "@event_name": "count", "number": $it }"""
                .asMessage()
                .let { primeNumberCounter.onMessage(it) }
        }

        primeNumberCounter.primes shouldBe 10 // ..23, 29
    }

    @Test
    fun `custom validation considers missing field as invalid value`() = runBlocking<Unit> {
        val orderListener = object : Subscriber() {
            var rColors = 0

            override fun subscribe() = Subscription.forEvent("order")
                .withValidation("color") { it.asText().contains("r") }

            override suspend fun receive(jsonMessage: JsonMessage) {
                rColors++
            }
        }

        """{ "@event_name": "order", "color": "red" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.rColors shouldBe 1

        """{ "@event_name": "order", "color": "blue" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.rColors shouldBe 1

        """{ "@event_name": "order" }""".trimMargin()
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.rColors shouldBe 1

        """{ "@event_name": "order", "color": "green" }"""
            .asMessage()
            .let { orderListener.onMessage(it) }

        orderListener.rColors shouldBe 2
    }

    private fun Int.isPrime(): Boolean {
        require(this in 1..100) { "Not valid for numbers outside range 1-100 inclusive" }

        val lowPrimes = listOf(2, 3, 5, 7)

        return when {
            this == 1 -> false
            this in lowPrimes -> true
            else -> (2..10).none { this % it == 0 }
        }
    }

    private fun String.asMessage(topic: String = "testTopic", key: String = UUID.randomUUID().toString()) =
        objectMapper.readTree(this).let {
            JsonMessage(
                eventName = it["@event_name"].asText(),
                json = it,
                metadata = EventMetadata(
                    topic = topic,
                    kafkaEvent = KafkaEvent(key, this),
                    createdAt = ZonedDateTime.now(),
                    readAt = ZonedDateTime.now()
                )
            )
        }

}
