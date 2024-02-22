package no.nav.helse.rapids_rivers

internal class KeyMessageContext(
    private val rapidsConnection: MessageContext,
    private val key: String?
) : MessageContext
