package no.nav.tms.kafka.application

class MinSideMdcConfig {
    var disable: Boolean = false
    lateinit var idFieldName: String
    lateinit var producedByFieldName: String
    lateinit var domain: Domain
    lateinit var idProducer: () -> String

    fun validate() {
        if (!disable) {
            val contextMessage = "Feil i MinSideMdcConfig: "
            val validIdField = ::idFieldName.isInitialized && idFieldName.isNotBlank()
            val validProducedByField = ::producedByFieldName.isInitialized && producedByFieldName.isNotBlank()
            val validDomainField = ::domain.isInitialized

            if (!validIdField || !validProducedByField || !validDomainField) {
                throw MinSideMdcConfigException(
                    contextMessage + "Følgende felt må være satt og ikke blanke: " +
                            "idFieldName, producedByFieldName, domain"
                )
            }
        }
    }

    fun describe(): String =
        """[idFieldName-> $idFieldName],[producedByFieldName-> $producedByFieldName],[domain-> ${domain.name}]
        """.trimIndent()


    fun initMinSideMdc(
        jsonMessage: JsonMessage,
    ): Map<String, String>? {
        return if (disable) null
        else {
            val domain: Domain by lazy { domain }
            val id = when {
                jsonMessage.json.has(idFieldName) -> jsonMessage[idFieldName].asText()
                ::idProducer.isInitialized -> {
                    val id = idProducer()
                    jsonMessage.putString(idFieldName, id)
                    id
                }
                else -> throw MinSideMdcConfigException(
                    "idProducer må være definert i MinSideMdcConfig eller feltet '$idFieldName' må være tilstede i jsonMessage for event '${jsonMessage.eventName}'"
                )
            }



            mapOf(
                "minside_id" to id,
                "domain" to domain.name,
                "produced_by" to jsonMessage[producedByFieldName].asText(),
                "event" to jsonMessage.eventName,
            )
        }
    }

    fun forSubscrpion(
        requiredFields: Set<String>,
        optionalFields: Set<String>,
        eventNames: List<String>
    ): MinSideMdcConfig? {
        if (disable)
            return null
        if (!requiredFields.contains(idFieldName)) {
            if (!::idProducer.isInitialized && optionalFields.contains(idFieldName)) {
                throw MinSideMdcConfigException(
                    "idProducer må være definert i MinSideMdcConfig når idFieldName '$idFieldName' er optional field i subscription for eventene ${
                        eventNames.joinToString(
                            ","
                        )
                    }. "
                )
            }
        }

        if (!requiredFields.contains(producedByFieldName)) {
            throw MinSideMdcConfigException(
                "Feltet '$producedByFieldName' er ikke definert i subscription for eventene ${eventNames.joinToString(", ")}. "
            )
        }

        return this

    }
}

class Domain private constructor(val name: String) {
    init {
        require(name.matches(Regex("^[a-z\\-]{4,15}\$"))) {
            "name må være 4-15 tegn og kan kun inneholde småbokstaver og -"
        }
    }

    companion object {
        val utkast = Domain("utkast")
        val varsel = Domain("varsel")
        val microfrontend = Domain("microfrontend")

        /**
         * Oppretter en custom Contenttype.NB! Kun for innhold som ikke er utkast, varsel eller microfrontend.
         * @param name  "verdi i`contenttype`feltet i loggene.Må være 1-15 tegn og kan kun inneholde småbokstaver og -"
         */
        fun custom(name: String) {
            require(
                name.lowercase().contains("utkast").not() &&
                        name.lowercase().contains("varsel").not() &&
                        name.lowercase().contains("microfrontend").not() &&
                        name.lowercase().contains("mikrofrontend").not()
            ) {
                "Bruk predefinerte Contenttype for utkast, varsel eller microfrontend"
            }
            Domain(name)
        }
    }
}

class MinSideMdcConfigException(message: String) : IllegalArgumentException(message)


