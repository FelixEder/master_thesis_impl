package systems.systemC

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import systems.systemC.dataclasses.PostalCertificate
import systems.iavSystem.database.dataclasses.PackageToSystemC
import spark.Spark.port
import spark.Spark.post
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger

/**
 * Simple service that handles the posting of information to the relevant recipients.
 *
 * @author Felix Eder
 * @date 2021-04-22
 */
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemC/database.csv",
    "src/main/kotlin/csv/systemC/incomingHttp.csv",
    "src/main/kotlin/csv/systemC/outgoingHttp.csv")
private val systemCCounter = TwoHeaderCsvLogger("src/main/kotlin/csv/systemC/iavControlPerPerson.csv",
    "description", "time (ms)")
private val postalService = PostalService(csvLogger, systemCCounter)
private val gson = Gson()

/**
 * Sets up the simple REST api to listen to requests.
 */
fun main() {
    setUpHttpServer()
}

/**
 * Sets up the simple REST api to listen to postal requests.
 */
private fun setUpHttpServer() {
    port(4572)

    post("/systemC/certGranted") { req, res ->
        csvLogger.logIncomingHttpRequest("POST", "Incoming post request to grant certificate in systemC", req.contentLength())

        val jsonObject: JsonObject = JsonParser.parseString(req.body()).asJsonObject

        val packageToSystemC = gson.fromJson(req.body(), PackageToSystemC::class.java)

        //val postalCertificate = PostalCertificate(jsonObject.get("id").asInt, jsonObject.get("personalNumber").asString,
        //jsonObject.get("dateIssued").asString, jsonObject.get("expirationDate").asString)

        println("Launching certificate postal assignment")
        GlobalScope.launch {
            postalService.startCertificateAssignment(packageToSystemC)
        }
        res.status(200)
        "ok"
    }

    post("/systemC/certUnregistered") { req, res ->
        csvLogger.logIncomingHttpRequest("POST", "Incoming post request to unregister certificate in systemC", req.contentLength())

        val jsonObject: JsonObject = JsonParser.parseString(req.body()).asJsonObject

        val postalCertificate = PostalCertificate(jsonObject.get("id").asInt, jsonObject.get("personalNumber").asString,
            jsonObject.get("dateIssued").asString, jsonObject.get("expirationDate").asString)

        println("Launching certificate unregister postal assignment")
        GlobalScope.launch {
            postalService.startCertificateUnregistered(postalCertificate)
        }
        res.status(200)
        "ok"
    }
}