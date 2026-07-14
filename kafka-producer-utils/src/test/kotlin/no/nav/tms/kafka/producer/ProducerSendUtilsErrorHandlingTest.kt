package no.nav.tms.kafka.producer

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.tms.kafka.producer.ProducerSendUtils.batched
import no.nav.tms.kafka.producer.ProducerSendUtils.sendSynchronized
import no.nav.tms.kafka.producer.ProducerSendUtils.transactional
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID


class ProducerSendUtilsErrorHandlingTest {

    @Nested
    inner class SynchronizedSendTest {
        @Test
        fun `sender FatalSendException ved autentiseringsfeil`() {
            val producer = mockProducer(true)

            producer.sendException = AuthenticationException("Feil passord!")

            val event = outputRecord("data")

            shouldThrow<FatalSendException> {
                producer.sendSynchronized(event)
            }
        }

        @Test
        fun `sender RetriableSendException ved andre kafka-feil`() {
            val producer = mockProducer(true)

            producer.sendException = KafkaException()

            val event = outputRecord("data")

            shouldThrow<RetriableSendException> {
                producer.sendSynchronized(event)
            }
        }

        @Test
        fun `sender FatalSendException ved ukjent feil`() {
            val producer = mockProducer(true)

            producer.sendException = RuntimeException()

            val event = outputRecord("data")

            shouldThrow<FatalSendException> {
                producer.sendSynchronized(event)
            }
        }

        @Test
        fun `sender RetriableSendException dersom persistering på kafka feiler`() {
            val producer = mockProducer(false)

            val event = outputRecord("data")

            runBlocking {
                async(Dispatchers.Unconfined) {
                    var completed = false

                    while (!completed) {
                        completed = producer.errorNext(KafkaException())
                        delay(100)
                    }
                }

                shouldThrow<RetriableSendException> {
                    producer.sendSynchronized(event)
                }
            }
        }

        @Test
        fun `sender RetriableSendException ved timeout`() {
            val producer = mockProducer(false)

            val event = outputRecord("data")

            shouldThrow<RetriableSendException> {
                producer.sendSynchronized(event, timeoutSeconds = 1)
            }
        }
    }

    @Nested
    inner class BatchedSendTest {

        @Test
        fun `kjører kun onSuccess for vellykkede records`() {
            val producer = mockProducer(false)

            // Prevent early completion
            producer.flushException = RuntimeException()

            val exception = KafkaException("Random kafka-feil!")

            val numEvents = 10
            val errorEvery = 2

            var successfulRecords = 0

            runBlocking {
                async(Dispatchers.Unconfined) {
                    var totalCompleted = 0

                    while (totalCompleted < numEvents) {
                        val completed = if (totalCompleted % errorEvery == 0) {
                            producer.errorNext(exception)
                        } else {
                            producer.completeNext()
                        }

                        if (completed) {
                            totalCompleted += 1
                        }

                        delay(10)
                    }
                }

                producer.batched {
                    (0 until numEvents).forEach { i ->
                        val data = """{"data": "abc-$i"}"""

                        val event = outputRecord(data)

                        sendInBatch(event) {
                            successfulRecords += 1
                        }
                    }
                }
            }

            successfulRecords shouldBe numEvents / errorEvery
        }

        @Test
        fun `kaster FatalSendException ved autentiseringsfeil`() {
            val producer = mockProducer(true)

            producer.sendException = AuthenticationException("Feil passord!")

            var successful = false

            shouldThrow<FatalSendException> {
                val event = outputRecord("data")

                producer.batched {
                    sendInBatch(event) {
                        successful = true
                    }
                }
            }

            successful shouldBe false
        }

        @Test
        fun `ignorerer andre exceptions uten å kjøre onSuccess`() {
            val producer = mockProducer(true)

            producer.sendException = RuntimeException("Random feil!")

            var successful = false

            shouldNotThrow<Exception> {
                val event = outputRecord("data")

                producer.batched {
                    sendInBatch(event) {
                        successful = true
                    }
                }
            }

            successful shouldBe false
        }

        @Test
        fun `sender RetriableSendException ved timeout`() {
            val producer = mockProducer(false)

            val event = outputRecord("data")

            shouldThrow<RetriableSendException> {
                producer.sendSynchronized(event, timeoutSeconds = 1)
            }
        }
    }

    @Nested
    inner class TransactionalSendTest {

        @Test
        fun `sender FatalSendException ved autentiseringsfeil`() {
            val producer = mockProducer(true).also {
                it.initTransactions()
            }
            val consumer = MockConsumer<String, String>("earliest")

            val inputTopic = "mock-input"
            val outputTopic = "mock-output"
            val partition = 0

            val topicPartition = TopicPartition(inputTopic, partition)

            consumer.assign(listOf(topicPartition))
            consumer.updateBeginningOffsets(mapOf(topicPartition to 0))

            val record1 = ConsumerRecord(inputTopic, partition, 1, "a",  "value-123")
            val record2 = ConsumerRecord(inputTopic, partition, 2, "b",  "value-123")
            val record3 = ConsumerRecord(inputTopic, partition, 3, "c",  "value-123")

            consumer.addRecord(record1)
            consumer.addRecord(record2)
            consumer.addRecord(record3)

            producer.sendException = AuthenticationException("Feil passord!")

            shouldThrow<FatalSendException> {
                consumer.poll(Duration.ofSeconds(1)).let { records ->
                    producer.transactional(records, consumer) {
                        records.forEach {
                            val outputRecord = ProducerRecord(outputTopic, it.key(), it.value())
                            sendInTransaction(outputRecord)
                        }
                    }
                }
            }

            producer.transactionAborted() shouldBe false
        }

        @Test
        fun `sender RetriableSendException ved andre kafka-feil under sending`() {
            val producer = mockProducer(true).also {
                it.initTransactions()
            }
            val consumer = MockConsumer<String, String>("earliest")

            val inputTopic = "mock-input"
            val outputTopic = "mock-output"
            val partition = 0

            val topicPartition = TopicPartition(inputTopic, partition)

            consumer.assign(listOf(topicPartition))
            consumer.updateBeginningOffsets(mapOf(topicPartition to 0))

            val record1 = ConsumerRecord(inputTopic, partition, 1, "a",  "value-123")
            val record2 = ConsumerRecord(inputTopic, partition, 2, "b",  "value-123")
            val record3 = ConsumerRecord(inputTopic, partition, 3, "c",  "value-123")

            consumer.addRecord(record1)
            consumer.addRecord(record2)
            consumer.addRecord(record3)

            producer.sendException = KafkaException("Tilfeldig kafka-feil!")

            shouldThrow<RetriableSendException> {
                consumer.poll(Duration.ofSeconds(1)).let { records ->
                    producer.transactional(records, consumer) {
                        records.forEach {
                            val outputRecord = ProducerRecord(outputTopic, it.key(), it.value())
                            sendInTransaction(outputRecord)
                        }
                    }
                }
            }

            producer.transactionAborted() shouldBe true
        }

        @Test
        fun `sender FatalSendException ved ukjent feil`() {
            val producer = mockProducer(true).also {
                it.initTransactions()
            }
            val consumer = MockConsumer<String, String>("earliest")

            val inputTopic = "mock-input"
            val outputTopic = "mock-output"
            val partition = 0

            val topicPartition = TopicPartition(inputTopic, partition)

            consumer.assign(listOf(topicPartition))
            consumer.updateBeginningOffsets(mapOf(topicPartition to 0))

            val record1 = ConsumerRecord(inputTopic, partition, 1, "a",  "value-123")
            val record2 = ConsumerRecord(inputTopic, partition, 2, "b",  "value-123")
            val record3 = ConsumerRecord(inputTopic, partition, 3, "c",  "value-123")

            consumer.addRecord(record1)
            consumer.addRecord(record2)
            consumer.addRecord(record3)

            producer.sendException = RuntimeException("Ukjent feil.")

            shouldThrow<FatalSendException> {
                consumer.poll(Duration.ofSeconds(1)).let { records ->
                    producer.transactional(records, consumer) {
                        records.forEach {
                            val outputRecord = ProducerRecord(outputTopic, it.key(), it.value())
                            sendInTransaction(outputRecord)
                        }
                    }
                }
            }

            producer.transactionAborted() shouldBe false
        }

        @Test
        fun `sender FatalSendException dersom producer er sperret fra transaksjonell handling`() {
            val producer = mockProducer(true).also {
                it.initTransactions()
            }
            val consumer = MockConsumer<String, String>("earliest")

            val inputTopic = "mock-input"
            val outputTopic = "mock-output"
            val partition = 0

            val topicPartition = TopicPartition(inputTopic, partition)

            consumer.assign(listOf(topicPartition))
            consumer.updateBeginningOffsets(mapOf(topicPartition to 0))

            val record1 = ConsumerRecord(inputTopic, partition, 1, "a",  "value-123")
            val record2 = ConsumerRecord(inputTopic, partition, 2, "b",  "value-123")
            val record3 = ConsumerRecord(inputTopic, partition, 3, "c",  "value-123")

            consumer.addRecord(record1)
            consumer.addRecord(record2)
            consumer.addRecord(record3)

            producer.commitTransactionException = ProducerFencedException("Produsent sperret!")

            shouldThrow<FatalSendException> {
                consumer.poll(Duration.ofSeconds(1)).let { records ->
                    producer.transactional(records, consumer) {
                        records.forEach {
                            val outputRecord = ProducerRecord(outputTopic, it.key(), it.value())
                            sendInTransaction(outputRecord)
                        }
                    }
                }
            }

            producer.transactionAborted() shouldBe false
        }
    }


    private fun outputRecord(data: String): ProducerRecord<String, String> =
        ProducerRecord("mock-output", UUID.randomUUID().toString(), data)

    // For å teste feilhåndtering må vi bruke mockproducer i stedet for testproducer for å trigge bestemte feil
    private fun mockProducer(autoComplete: Boolean = true) = MockProducer(
        autoComplete,
        null,
        StringSerializer(),
        StringSerializer()
    )
}
