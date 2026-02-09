package no.nav.tms.kafka.application

import io.github.oshai.kotlinlogging.withLoggingContext
import no.nav.tms.kafka.application.JsonMessage.Companion.DEFAULT_EVENT_NAME

class MinSideMdcConfig {
    var disable: Boolean = false
    var eventFieldName: String = DEFAULT_EVENT_NAME
    lateinit var idFieldName: String
    lateinit var producedByFieldName: String
    lateinit var domain: Domain

    fun validate() {
        if (!disable) {
            val contextMessage = "Feil i MinSideMdcConfig: "
            val validIdField = ::idFieldName.isInitialized && idFieldName.isNotBlank()
            val validProducedByField = ::producedByFieldName.isInitialized && producedByFieldName.isNotBlank()
            val validDomainField = ::domain.isInitialized

            if (!validIdField || !validProducedByField || !validDomainField) {
                throw IllegalArgumentException(
                    contextMessage + "Følgende felt må være satt og ikke blanke: " +
                            "idFieldName, producedByFieldName, domain"
                )
            }
        }
    }

    fun describe(): String =
        """[eventFieldName -> $eventFieldName],[idFieldName-> $idFieldName],[producedByFieldName-> $producedByFieldName],[domain-> ${domain.name}]
        """.trimIndent()

    fun initMinSideContext(
        jsonMessage: JsonMessage,
    ) = object : MinSideContext {
        override val eventName: String = jsonMessage.getOrNull(eventFieldName)?.asText()
            ?: throw IllegalArgumentException("Event name field '${eventFieldName}' not found in message")
        override val domain: Domain by lazy { domain }
        override val producedBy: String = jsonMessage.getOrNull(producedByFieldName)?.asText()
            ?: throw IllegalArgumentException("Produced by field '${producedByFieldName}' not found in message")
        override val extraFields: Map<String, String>? = null
        override val minSideId: String = jsonMessage.getOrNull(idFieldName)?.asText()
            ?: throw IllegalArgumentException("ID field '${idFieldName}' not found in message")
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

/**
 * Interface for å definere kontekst for MinSide-logger. Kan implementeres av objekter som skal logges for å enkelt kunne legge til relevant kontekst i loggene.
 * Inneholder predefinerte felter minside_id, contenttype og produced_by, samt mulighet for ekstra felter ved behov.
 */

interface MinSideContext {
    val domain: Domain
    val producedBy: String
    val extraFields: Map<String, String>?
    val minSideId: String
    val eventName: String

    fun toMap(): Map<String, String> {
        val basemap = mapOf(
            "minside_id" to minSideId,
            "contenttype" to domain.name,
            "produced_by" to producedBy,
            "event" to eventName,
        )

        return extraFields?.let {
            return basemap + it
        } ?: basemap
    }
}


/**
 * Legg til kontekst på loki-logger
 * @param [minSideId] id for sporing gjennom loggene fra alle tjenester
 * @param[producedBy] team som produserte innholdet
 * @param[domain] type innhold som logges, f.eks varsel, utkast eller microfrontend. Kan også opprettes custom ved behov
 *@param[function] kodeblokk som skal kjøres med denne konteksten
 */
/**
 * Legg til kontekst for loki-logger fra et MinSideContext objekt
 * @param[minSideContext] objekt som implementerer MinSideContext
 */

internal suspend fun <T : MinSideContext> withMinSideMdc(
    minSideContext: T,
    function:suspend () -> SubscriberResult
): SubscriberResult {
    withLoggingContext(
        restorePrevious = false,
        map = minSideContext.toMap()
    ) { return function() }
}




