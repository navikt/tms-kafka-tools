package no.nav.tms.kafka.application

internal class MinSideMdcConfig(
    val domain: Domain,
    val idSupplier: (JsonMessage) -> String,
    val producedBySupplier: (JsonMessage) -> String?,
    val description: String
) {

    fun mdcMapFromMessage(
        jsonMessage: JsonMessage,
    ): Map<String, String> {

        val mdcMap = mutableMapOf(
            "minside_id" to idSupplier(jsonMessage),
            "domain" to domain.name,
            "event" to jsonMessage.eventName,
        )

        producedBySupplier(jsonMessage)?.let { producer ->
            mdcMap["produced_by"] = producer
        }

        return mdcMap
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
        fun custom(name: String): Domain {
            require(
                name.lowercase().contains("utkast").not() &&
                        name.lowercase().contains("varsel").not() &&
                        name.lowercase().contains("microfrontend").not() &&
                        name.lowercase().contains("mikrofrontend").not()
            ) {
                "Bruk predefinerte Contenttype for utkast, varsel eller microfrontend"
            }
            return Domain(name)
        }
    }
}

class MinSideMdcConfigException(message: String) : IllegalArgumentException(message)


