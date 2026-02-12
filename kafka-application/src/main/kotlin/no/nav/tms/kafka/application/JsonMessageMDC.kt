package no.nav.tms.kafka.application

internal class MinSideMdcConfig(
    val idFieldName: String,
    val producedByFieldName: String,
    val domain: Domain
) {
    fun describe() = "[idFieldName=$idFieldName, producedByFieldName=$producedByFieldName, domain-> ${domain.name}]"

    fun mdcMapFromMessage(
        jsonMessage: JsonMessage,
    ): Map<String, String> {
        val mdcMap = mutableMapOf(
            "minside_id" to jsonMessage[idFieldName].asText(),
            "domain" to domain.name,
            "event" to jsonMessage.eventName,
        )

        jsonMessage.getOrNull(producedByFieldName)?.let {
            mdcMap["produced_by"] = it.asText()
        }

        return mdcMap
    }

    fun validateSubscription(subscribedFields: MutableSet<String>, eventNames: List<String>) {
        if (!subscribedFields.contains(idFieldName)) {
            throw MinSideMdcConfigException(
                "Id-felt '$idFieldName' er ikke definert i subscription for eventene $eventNames."
            )
        }

        if (!subscribedFields.contains(producedByFieldName)) {
            throw MinSideMdcConfigException(
                "Producer-felt '$producedByFieldName' er ikke definert i subscription for eventene $eventNames."
            )
        }
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


