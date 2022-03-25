package no.nav.helse.rapids_rivers

class River(rapidsConnection: RapidsConnection) : RapidsConnection.MessageListener {
    private val validations = mutableListOf<PacketValidation>()

    private val listeners = mutableListOf<PacketListener>()

    init {
        rapidsConnection.register(this)
    }

    fun validate(validation: PacketValidation): River {
        validations.add(validation)
        return this
    }

    fun onSuccess(listener: PacketValidationSuccessListener): River {
        listeners.add(DelegatedPacketListener(listener))
        return this
    }

    fun onError(listener: PacketValidationErrorListener): River {
        listeners.add(DelegatedPacketListener(listener))
        return this
    }

    fun register(listener: PacketListener): River {
        listeners.add(listener)
        return this
    }

    override fun onMessage(message: String, context: MessageContext) {
        val problems = MessageProblems(message)
        try {
            val packet = JsonMessage(message, problems)
            validations.forEach { it.validate(packet) }
            if (problems.hasErrors()) {
                return onError(problems, context)
            }
            onPacket(packet, context)
        } catch (err: MessageProblems.MessageException) {
            return onSevere(err, context)
        }
    }

    private fun onPacket(packet: JsonMessage, context: MessageContext) {
        packet.interestedIn("@event_name")
        listeners.forEach {
            val riverName = it.javaClass.simpleName
            val eventName = packet["@event_name"].textValue() ?: "ukjent"
            Metrics.onPacketHistorgram.labels(
                context.rapidName(),
                riverName,
                eventName
            ).time {
                it.onPacket(packet, context)
            }
            Metrics.onMessageCounter.labels(context.rapidName(), riverName, "ok").inc()
        }
    }

    private fun onSevere(error: MessageProblems.MessageException, context: MessageContext) {
        listeners.forEach {
            val riverName = it.javaClass.simpleName
            Metrics.onMessageCounter.labels(context.rapidName(), riverName, "severe").inc()
            it.onSevere(error, context)
        }
    }

    private fun onError(problems: MessageProblems, context: MessageContext) {
        listeners.forEach {
            val riverName = it.javaClass.simpleName
            Metrics.onMessageCounter.labels(context.rapidName(), riverName, "error").inc()
            it.onError(problems, context)
        }
    }

    fun interface PacketValidation {
        fun validate(message: JsonMessage)
    }

    fun interface PacketValidationSuccessListener {
        fun onPacket(packet: JsonMessage, context: MessageContext)
    }

    fun interface PacketValidationErrorListener {
        fun onError(problems: MessageProblems, context: MessageContext)
    }

    interface PacketListener : PacketValidationErrorListener, PacketValidationSuccessListener {
        override fun onError(problems: MessageProblems, context: MessageContext) {}

        fun onSevere(error: MessageProblems.MessageException, context: MessageContext) {}
    }

    private class DelegatedPacketListener private constructor(
        private val packetHandler: PacketValidationSuccessListener,
        private val errorHandler: PacketValidationErrorListener
    ) : PacketListener {
        constructor(packetHandler: PacketValidationSuccessListener) : this(packetHandler, { _, _ -> })
        constructor(errorHandler: PacketValidationErrorListener) : this({ _, _ -> }, errorHandler)

        override fun onError(problems: MessageProblems, context: MessageContext) {
            errorHandler.onError(problems, context)
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            packetHandler.onPacket(packet, context)
        }
    }
}
