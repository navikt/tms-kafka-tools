package no.nav.tms.kafka.application

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test


class MessageTrackerTest {
    object AppleCollector : Subscriber() {
        var count = 0

        override fun subscribe() = Subscription.forEvent("order")
            .withValue("item", "apple")
            .withFields("price", "color")
            .withOptionalFields("referenceId")

        override suspend fun receive(jsonMessage: JsonMessage) {
            validateColor(jsonMessage["color"].asText())
            validatePrice(jsonMessage["price"].asDouble())
            count++
        }

        fun validateColor(color: String) {
            when (color) {
                "red", "green", "yellow" -> return
                else -> throw AppleColorException()
            }
        }

        fun validatePrice(price: Double) {
            if (price !in 0.0..20.0) {
                throw ApplePriceException()
            }
        }
    }

    class ApplePriceException : SkippableMessageException("Price of apple too high or negative")
    class AppleColorException : SkippableMessageException("Apple is of suspicious color")

    private val broadcaster = MessageBroadcaster(AppleCollector, enableTracking = true)

    @AfterEach
    fun cleanUp() {
        broadcaster.clearHistory()
    }

    @Test
    fun `allows for tracking message outcomes`() {

        """{ "@event_name": "registered" }"""
            .let { broadcaster.broadcastJson(it) }

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "red" }"""
            .let { broadcaster.broadcastJson(it) }

        """{ "@event_name": "order", "item": "apple", "price": 80.0, "color": "red" }"""
            .let { broadcaster.broadcastJson(it) }

        """{ "@event_name": "delivery" }"""
            .let { broadcaster.broadcastJson(it) }

        broadcaster.history().collectAggregate(AppleCollector::class).let {
            it.shouldNotBeNull()
            it.skipped shouldBe 1
            it.ignored shouldBe 2
            it.accepted shouldBe 1
        }
    }

    @Test
    fun `allows for tracking outcome for specific messages`() {

        """{ "@event_name": "registered" }"""
            .let { broadcaster.broadcastJson(it) }

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "red" }"""
            .let { broadcaster.broadcastJson(it) }

        broadcaster.history().findOutcome(AppleCollector::class) {
            it.eventName == "registered"
        }.let {
            it.shouldNotBeNull()
            it.status shouldBe MessageStatus.Ignored
        }

        broadcaster.history().findOutcome(AppleCollector::class) {
            it.eventName == "order"
        }.let {
            it.shouldNotBeNull()
            it.status shouldBe MessageStatus.Accepted
        }
    }

    @Test
    fun `allows for detailed tracking of skipped outcomes`() {

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "red", "referenceId": "id-1" }"""
            .let { ok -> broadcaster.broadcastJson(ok) }

        """{ "@event_name": "order", "item": "apple", "price": 25.0, "color": "red", "referenceId": "id-2" }"""
            .let { pricy -> broadcaster.broadcastJson(pricy) }

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "purple", "referenceId": "id-3" }"""
            .let { purple -> broadcaster.broadcastJson(purple) }

        broadcaster.history().findOutcome(AppleCollector::class) {
            it["referenceId"].asText() == "id-1"
        }.let {
            it.shouldBeInstanceOf<MessageAccepted>()
            it.shouldNotBeNull()
            it.status shouldBe MessageStatus.Accepted
        }

        broadcaster.history().findOutcome(AppleCollector::class) {
            it["referenceId"].asText() == "id-2"
        }.let {
            it.shouldBeInstanceOf<MessageSkipped>()
            it.status shouldBe MessageStatus.Skipped

            it.cause.shouldBeInstanceOf<ApplePriceException>()
        }

        broadcaster.history().findOutcome(AppleCollector::class) {
            it["referenceId"].asText() == "id-3"
        }.let {
            it.shouldBeInstanceOf<MessageSkipped>()
            it.status shouldBe MessageStatus.Skipped

            it.cause.shouldBeInstanceOf<AppleColorException>()
        }
    }

    @Test
    fun `allows for finding only skipped outcomes`() {

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "red", "referenceId": "id-1" }"""
            .let { ok -> broadcaster.broadcastJson(ok) }

        """{ "@event_name": "order", "item": "apple", "price": 25.0, "color": "red", "referenceId": "id-2" }"""
            .let { pricy -> broadcaster.broadcastJson(pricy) }

        broadcaster.history().findSkippedOutcome(AppleCollector::class) {
            it["color"].asText() == "red"
        }.let {
            it.shouldNotBeNull()
            it.status shouldBe MessageStatus.Skipped
            it.cause.shouldBeInstanceOf<ApplePriceException>()
        }
    }

    @Test
    fun `deprecated field 'failed' should equal 'skipped'`() {
        """{ "@event_name": "order", "item": "apple", "price": 25.0, "color": "red", "referenceId": "id-2" }"""
            .let { pricy -> broadcaster.broadcastJson(pricy) }

        """{ "@event_name": "order", "item": "apple", "price": 25.0, "color": "indigo", "referenceId": "id-2" }"""
            .let { indigo -> broadcaster.broadcastJson(indigo) }

        """{ "@event_name": "order", "item": "apple", "price": 8.0, "color": "purple", "referenceId": "id-3" }"""
            .let { purple -> broadcaster.broadcastJson(purple) }

        val history = broadcaster.history()
        val appleColl = AppleCollector::class

        history.collectAggregate(appleColl).let {
            it.shouldNotBeNull()
            it.failed shouldBe it.skipped
        }

        history.allFailedOutcomes(appleColl) shouldBe history.allSkippedOutcomes(appleColl)

        history.findFailedOutcome(appleColl) { it["color"].asText() == "indigo" } shouldBe
            history.findSkippedOutcome(appleColl) { it["color"].asText() == "indigo" }

        history.findFailedOutcome(appleColl) { it["color"].asText() == "red" } shouldBe
            history.findSkippedOutcome(appleColl) { it["color"].asText() == "red" }

        history.findFailedOutcome(appleColl) { it["price"].asDouble() == 25.0 } shouldBe
            history.findSkippedOutcome(appleColl) { it["price"].asDouble() == 25.0 }
    }
}
