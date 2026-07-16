package no.nav.tms.kafka.producer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.prometheus.metrics.core.metrics.Counter
import no.nav.tms.common.logging.TeamLogs
import no.nav.tms.kafka.producer.Metrics.SendMode.Synchronized
import no.nav.tms.kafka.producer.Metrics.SendMode.Batched
import no.nav.tms.kafka.producer.Metrics.SendMode.Transactional
import no.nav.tms.kafka.producer.Metrics.FailureStage.Commit
import no.nav.tms.kafka.producer.Metrics.FailureStage.Flush
import no.nav.tms.kafka.producer.Metrics.FailureStage.Send
import no.nav.tms.kafka.producer.Metrics.FailureStage.Sync
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import java.lang.IllegalStateException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

object ProducerSendUtils {

    private val log = KotlinLogging.logger { }
    private val teamLog = TeamLogs.logger(failSilently = true) { }

    fun <K, V> Producer<K, V>.sendSynchronized(record: ProducerRecord<K, V>, timeoutSeconds: Long = 15) {
        val future = try {
            send(record)
        } catch (e: KafkaException) {
            Metrics.registerFailedSend(Synchronized, Send)

            when (e) {
                is AuthenticationException -> throw FatalSendException("Feil ved autentisering mot kafka", e)
                else -> throw RetriableSendException("Feil ved synkron sending av kafka-record", e)
            }
        } catch (e: Exception) {
            throw FatalSendException("Ukjent feil ved synkron sending av record til kafka", e)
        }

        try {
            val result = future.get(timeoutSeconds, TimeUnit.SECONDS)
            assert (result.hasOffset()) { "Ingen offset i metadata fra kafka" }
        } catch (e: Exception) {
            Metrics.registerFailedSend(Synchronized, Sync)
            throw RetriableSendException("Kafka-record ble ikke persistert på kafka", e)
        }

        Metrics.registerSynchronizedSend()
    }

    fun <K, V> Producer<K, V>.batched(timeoutSeconds: Long = 15, batched: Batch<K, V>.() -> Unit) {
        Batch(timeoutSeconds, this)
            .also(batched)
            .let(Batch<K, V>::completeBatch)
    }

    class Batch<K, V> internal constructor(
        private val syncTimeoutSeconds: Long,
        private val kafkaProducer: Producer<K, V>
    ) {
        init {
            Metrics.registerBatchStarted()
        }

        private val results = mutableListOf<Pair<Future<RecordMetadata>, () -> Unit>>()

        fun sendInBatch(record: ProducerRecord<K, V>, onSuccess: () -> Unit) {
            try {
                results += kafkaProducer.send(record) to onSuccess
            } catch (ae: AuthenticationException) {
                Metrics.registerFailedSend(Batched, Send)
                log.error { "Fikk autentiseringsfeil ved sending av batchet kafka-record" }
                teamLog.error(ae) { "Fikk autentiseringsfeil ved sending av batchet kafka-record" }

                throw FatalSendException("Feil ved autentisering mot kafka", ae)
            } catch (e: Exception) {
                Metrics.registerFailedSend(Batched, Send)
                log.error { "Fikk feil ved sending av batchet kafka-record" }
                teamLog.error(e) { "Fikk feil ved sending av batchet kafka-record" }
            }
        }

        internal fun completeBatch() {
            try {
                kafkaProducer.flush()
            } catch (e: Exception) {
                Metrics.registerFailedSend(Batched, Flush)
                log.error { "Fikk feil ved flushing av batch med kafka-records. Fortsetter prosessering." }
                teamLog.error(e) { "Fikk feil ved flushing av batch med kafka-records. Fortsetter prosessering." }
            }

            results.forEach { (result, callback) ->
                try {
                    val offsetMetadata = result.get(syncTimeoutSeconds, TimeUnit.SECONDS)
                    if (offsetMetadata.hasOffset()) {
                        Metrics.registerBatchedSend()
                        callback()
                    } else {
                        Metrics.registerFailedSend(Batched, Sync)
                        log.warn { "Batch-record ble ikke godtatt av kafka av ukjent årsak." }
                    }
                } catch (e: Exception) {
                    Metrics.registerFailedSend(Batched, Sync)
                    log.error { "Fikk feil ved synkronisering av batch med records til kafka" }
                    teamLog.error(e) { "Fikk feil ved synkronisering av batch med records til kafka" }
                }
            }
        }
    }

    fun <K, V> Producer<K, V>.transactional(
        consumerRecords: ConsumerRecords<*, *>,
        consumer: Consumer<*, *>,
        transactional: Transaction<K, V>.() -> Unit
    ) {
        beginTransaction()

        Transaction(consumerRecords, consumer.groupMetadata(), this)
            .also(transactional)
            .let(Transaction<K, V>::completeTransaction)
    }

    class Transaction<K, V> internal constructor(
        consumerRecords: ConsumerRecords<*, *>,
        private val consumerGroup: ConsumerGroupMetadata,
        private val kafkaProducer: Producer<K, V>
    ) {
        init {
            Metrics.registerTransactionStarted()
        }

        private var transactionSize: Long = 0

        private val transactionalConsumerOffsets = transactionOffsets(consumerRecords)

        fun sendInTransaction(record: ProducerRecord<K, V>) {
            try {
                kafkaProducer.send(record)
                transactionSize += 1
            } catch (e: Exception) {

                Metrics.registerFailedSend(Transactional, Send)
                when (e) {
                    is AuthenticationException -> throw FatalSendException("Feil ved autentisering mot kafka", e)
                    is KafkaException -> {
                        kafkaProducer.abortTransaction()
                        throw RetriableSendException("Midlertidig feil ved transaksjonell sending av kafka-record", e)
                    }
                    else -> throw FatalSendException("Ukjent feil ved transaksjonell sending av kafka-record", e)
                }
            }
        }

        internal fun completeTransaction() {
            try {
                kafkaProducer.sendOffsetsToTransaction(transactionalConsumerOffsets, consumerGroup)
                kafkaProducer.commitTransaction()
                Metrics.registerTransactionalSend(transactionSize)
            } catch (ke: KafkaException) {
                Metrics.registerFailedSend(Transactional, Commit)
                when (ke) {
                    is ProducerFencedException, is OutOfOrderSequenceException, is AuthenticationException -> {
                        log.error { "Fatal feil ved transaksjonell sending av kafka-eventer." }
                        throw FatalSendException("Fatal feil ved transaksjonell sending av kafka-eventer.", ke)
                    }
                    else -> {
                        log.warn { "Midlertidig feil ved transaksjonell sending av kafka-eventer. Ruller tilbake transaksjon." }
                        kafkaProducer.abortTransaction()
                    }
                }
            } catch (e: Exception) {
                Metrics.registerFailedSend(Transactional, Commit)
                log.error(e) { "Fatal feil ved transaksjonell sending av kafka-eventer." }
                throw e
            }
        }

        private fun transactionOffsets(records: ConsumerRecords<*, *>): Map<TopicPartition, OffsetAndMetadata> {
            val nextOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()

            records.forEach { record ->
                nextOffsets[record.topicPartition()] = OffsetAndMetadata(record.offset() + 1)
            }

            return nextOffsets
        }

        private fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(topic(), partition())
    }
}

internal object Metrics {
    private const val NAMESPACE = "tms_kafka_producer"

    enum class SendMode {
        Synchronized, Batched, Transactional
    }

    enum class FailureStage {
        Send, Flush, Sync, Commit
    }

    fun registerSynchronizedSend() =
        RECORD_SENT.labelValues(Synchronized.name.lowercase()).inc()

    fun registerBatchStarted() =
        BATCHES_STARTED.inc()
    fun registerBatchedSend() =
        RECORD_SENT.labelValues(Batched.name.lowercase()).inc()

    fun registerTransactionStarted() =
        TRANSACTIONS_STARTED.inc()
    fun registerTransactionalSend(count: Long) =
        RECORD_SENT.labelValues(Transactional.name.lowercase()).inc(count)

    fun registerFailedSend(mode: SendMode, stage: FailureStage) =
        FAILED_SEND.labelValues(mode.name.lowercase(), stage.name.lowercase()).inc()

    private const val RECORD_SENT_NAME = "${NAMESPACE}_record_sent"

    private const val BATCHES_STARTED_NAME = "${NAMESPACE}_batches_started"
    private const val TRANSACTIONS_STARTED_NAME = "${NAMESPACE}_transactions_started"

    private const val FAILED_SEND_NAME = "${NAMESPACE}_failed_send"

    private val RECORD_SENT: Counter = Counter.builder()
        .name(RECORD_SENT_NAME)
        .help("Antall kafka-records sendt per sendingsmetode")
        .labelNames("mode")
        .register()

    private val BATCHES_STARTED: Counter = Counter.builder()
        .name(BATCHES_STARTED_NAME)
        .help("Antall synkrone batcher påbegynt")
        .register()

    private val TRANSACTIONS_STARTED: Counter = Counter.builder()
        .name(TRANSACTIONS_STARTED_NAME)
        .help("Antall transaksjoner startet")
        .register()

    private val FAILED_SEND: Counter = Counter.builder()
        .name(FAILED_SEND_NAME)
        .help("Feil i sending av kafka-records")
        .labelNames("mode", "stage")
        .register()
}

class RetriableSendException(msg: String, cause: Exception? = null): RuntimeException(msg, cause)
class FatalSendException(msg: String, cause: Exception? = null): IllegalStateException(msg, cause)
