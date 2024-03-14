package no.nav.helse.rapids_rivers

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.*
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

class KafkaReader(
    factory: ConsumerFactory,
    groupId: String,
    private val kafkaTopics: List<String>,
    consumerProperties: Properties = Properties()
) : KafkaConnection(), ConsumerRebalanceListener {

    private val log = KotlinLogging.logger {}
    private val secureLog = KotlinLogging.logger("secureLog")

    private var state = State.Init

    private val consumer = factory.createConsumer(groupId, consumerProperties)

    init {
        log.info { "rapid initialized" }
    }

    fun isRunning() = state == State.Running

    override fun start() {
        log.info { "starting rapid" }

        when(state) {
            State.Running -> log.info { "rapid already started" }
            else -> {
                state = State.Running
                consumeMessages()
            }
        }
    }

    override fun stop() {
        log.info { "stopping rapid" }

        when(state) {
            State.Stopped -> log.info { "rapid already stopped" }
            else -> {
                state = State.Stopped
                consumer.wakeup()
            }
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

    private fun onRecords(records: ConsumerRecords<String, String>) {
        if (records.isEmpty) {
            return // poll returns an empty collection in case of rebalancing
        }

        val currentPositions = records
            .groupBy { it.topicPartition() }
            .mapValues { it.value.minOf { it.offset() } }
            .toMutableMap()

        try {
            records.forEach { record ->
                onRecord(record)
                currentPositions[record.topicPartition()] = record.offset() + 1
            }
        } catch (err: Exception) {
            log.info { "due to an error during processing, positions are reset to each next message." }
            secureLog.info(err) {
                "due to an error during processing, positions are reset to each next message (after each record that was processed OK):" +
                    currentPositions.map { "\tpartition=${it.key}, offset=${it.value}" }
                        .joinToString(separator = "\n", prefix = "\n", postfix = "\n")
            }
            currentPositions.forEach { (partition, offset) -> consumer.seek(partition, offset) }
            throw err
        } finally {
            consumer.commitSync(currentPositions.mapValues { (_, offset) -> OffsetAndMetadata(offset) })
        }
    }

    private fun onRecord(record: ConsumerRecord<String, String>) = withMDC(recordDiganostics(record)) {
        when (record.value()) {
            null -> log.info { "ignoring record with offset ${record.offset()} in partition ${record.partition()} because value is null (tombstone)" }
            else -> notifyMessage(NewJsonMessage.initMessage(record))
        }
    }


    private fun consumeMessages() {
        var lastException: Exception? = null
        try {
            consumer.subscribe(kafkaTopics, this)
            while (state == State.Running) {
                consumer.poll(Duration.ofSeconds(1)).also {
                    withMDC(pollDiganostics(it)) {
                        onRecords(it)
                    }
                }
            }
        } catch (err: WakeupException) {
            // throw exception if we have not been told to stop
            if (state != State.Stopped){
                throw err
            }
        } catch (err: Exception) {
            lastException = err
            throw err
        } finally {
            closeResources(lastException)
        }
    }

    private fun pollDiganostics(records: ConsumerRecords<String, String>) = mapOf(
        "rapids_poll_id" to "${UUID.randomUUID()}",
        "rapids_poll_time" to "${LocalDateTime.now()}",
        "rapids_poll_count" to "${records.count()}"
    )

    private fun recordDiganostics(record: ConsumerRecord<String, String>) = mapOf(
        "rapids_record_id" to "${UUID.randomUUID()}",
        "rapids_record_before_notify_time" to "${LocalDateTime.now()}",
        "rapids_record_produced_time" to "${record.timestamp()}",
        "rapids_record_produced_time_type" to "${record.timestampType()}",
        "rapids_record_topic" to record.topic(),
        "rapids_record_partition" to "${record.partition()}",
        "rapids_record_offset" to "${record.offset()}"
    )

    private fun TopicPartition.commitSync() {
        val offset = consumer.position(this)
        log.info { "committing offset offset=$offset for partition=$this" }
        consumer.commitSync(mapOf(this to OffsetAndMetadata(offset)))
    }

    private fun closeResources(lastException: Exception?) {
        state = State.Stopped
        if (lastException != null) {
            log.warn{ "stopped consuming messages due to an error" }
            secureLog.warn(lastException) { "stopped consuming messages due to an error" }
        } else {
            log.info { "stopped consuming messages after receiving stop signal" }
        }
        tryAndLog(consumer::close)
    }

    private fun tryAndLog(block: () -> Unit) {
        try {
            block()
        } catch (err: Exception) {
            log.error(err) { err.message }
        }
    }

    internal fun getMetrics() = listOf(KafkaClientMetrics(consumer))

    private fun ConsumerRecord<*, *>.topicPartition() = TopicPartition(topic(), partition())

    private enum class State {
        Init, Running, Stopped
    }
}
