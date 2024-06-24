package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import kotlinx.coroutines.*
import kotlinx.coroutines.CoroutineStart.LAZY
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.*
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

internal class KafkaReader(
    factory: ConsumerFactory,
    groupId: String,
    private val kafkaTopics: List<String>,
    private val broadcaster: RecordBroadcaster,
): ConsumerRebalanceListener {

    private val scope = CoroutineScope(Dispatchers.Default + Job())
    private val job = scope.launch(start = LAZY) { consumeMessages() }

    private val log = KotlinLogging.logger {}
    private val secureLog = KotlinLogging.logger("secureLog")

    private val consumer = factory.createConsumer(groupId)

    fun isRunning() = job.isActive

    fun start(wait: Boolean = true) {
        log.info { "starting kafka reader" }

        job.start()

        if (wait) {
            runBlocking {
                job.join()
            }
        }
    }

    fun stop() = runBlocking {
        log.info { "stopping kafka reader" }

        job.cancelAndJoin()
    }

    private suspend fun consumeMessages() {
        var lastException: Exception? = null
        try {
            consumer.subscribe(kafkaTopics, this)
            while (job.isActive) {

                withContext(Dispatchers.IO) {
                    consumer.poll(Duration.ofSeconds(1))
                }.also {
                    withMDC(pollDiganostics(it)) {
                        onRecords(it)
                    }
                }
            }
        } catch (err: WakeupException) {
            lastException = err
        } catch (err: Exception) {
            lastException = err
            consumer.wakeup()
        } finally {
            closeResources(lastException)
        }
    }

    private suspend fun onRecords(records: ConsumerRecords<String, String>) {
        if (records.isEmpty) {
            return // poll returns an empty collection in case of rebalancing
        }

        val currentPositions = records
            .groupBy { it.topicPartition() }
            .mapValues { it.value.minOf { it.offset() } }
            .toMutableMap()

        try {
            records.forEach { record ->
                currentPositions[record.topicPartition()] = record.offset()
                onRecord(record)
            }
        } catch (err: Exception) {

            log.info { "committing local offsets prematurely due to an error during processing" }
            secureLog.info(err) {
                "committing local offsets prematurely due to an error during processing" +
                    currentPositions.map { "\tpartition=${it.key}, offset=${it.value}" }
                        .joinToString(separator = "\n", prefix = "\n", postfix = "\n")
            }
            currentPositions.forEach { (partition, offset) -> consumer.seek(partition, offset) }
            throw err
        } finally {
            consumer.commitSync(currentPositions.mapValues { (_, offset) -> OffsetAndMetadata(offset) })
        }
    }

    private suspend fun onRecord(record: ConsumerRecord<String, String>) = withMDC(recordDiganostics(record)) {
        when (record.value()) {
            null -> log.info { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because value is null (tombstone)" }
            else -> broadcaster.broadcastRecord(record)
        }
    }

    private fun pollDiganostics(records: ConsumerRecords<String, String>) = mapOf(
        "kafka_poll_id" to "${UUID.randomUUID()}",
        "kafka_poll_time" to "${nowAtUtc()}",
        "kafka_poll_count" to "${records.count()}"
    )

    private fun recordDiganostics(record: ConsumerRecord<String, String>) = mapOf(
        "kafka_record_id" to "${UUID.randomUUID()}",
        "kafka_record_before_notify_time" to "${nowAtUtc()}",
        "kafka_record_produced_time" to "${record.timestamp()}",
        "kafka_record_produced_time_type" to "${record.timestampType()}",
        "kafka_record_topic" to record.topic(),
        "kafka_record_partition" to "${record.partition()}",
        "kafka_record_offset" to "${record.offset()}"
    )

    private fun TopicPartition.commitSync() {
        val offset = consumer.position(this)
        log.info { "committing offset offset=$offset for partition=$this" }
        consumer.commitSync(mapOf(this to OffsetAndMetadata(offset)))
    }

    private fun closeResources(lastException: Exception?) {
        if (lastException != null) {
            log.warn{ "stopped consuming messages due to an error" }
            secureLog.warn(lastException) { "stopped consuming messages due to an error" }
        } else {
            log.info { "stopped consuming messages after receiving stop signal" }
        }
        job.cancel()
        try {
            consumer.close()
        } catch (err: Exception) {
            log.error(err) { err.message }
        }
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        if (partitions.isNotEmpty()) {
            log.info { "partitions assigned: $partitions" }
        }
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        log.info {"partitions revoked: $partitions" }
        partitions.forEach { it.commitSync() }
    }

    internal fun getMetrics() = listOf(KafkaClientMetrics(consumer))

    private fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(topic(), partition())

    private fun nowAtUtc() = ZonedDateTime.now(ZoneId.of("Z"))
}
