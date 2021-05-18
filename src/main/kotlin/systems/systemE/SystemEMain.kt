package systems.systemE

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonParser
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.systemE.dataclasses.SystemEIavPayslip
import spark.Spark.port
import spark.Spark.post
import util.logging.CsvLogger
import java.net.BindException
import java.net.ConnectException
import java.util.concurrent.LinkedBlockingQueue

/**
 * A simple web service that gets monthly income reports from employers if there is a
 *
 * @author Felix Eder
 * @date 2021-04-20
 */

private val gson = Gson()
private val csvLogger = CsvLogger("src/main/kotlin/csv/SystemE/database.csv",
    "src/main/kotlin/csv/SystemE/incomingHttp.csv",
    "src/main/kotlin/csv/SystemE/outgoingHttp.csv")

fun main() {
    val blockingQueue = LinkedBlockingQueue<List<SystemEIavPayslip>>()
    setUpHttpServer(blockingQueue)
    GlobalScope.launch {
        sendInfoToSystemF(blockingQueue)
    }
}

/**
 * Sets up a simple HTTP server that listens for POST-requests from employers.
 */
private fun setUpHttpServer(blockingQueue: LinkedBlockingQueue<List<SystemEIavPayslip>>) {
    port(4571)

    post("/SystemE/payslip") { req, res ->
        csvLogger.logIncomingHttpRequest("POST",
            "Incoming post request with employer payslips",
            req.contentLength())

        val jsonArray: JsonArray = JsonParser.parseString(req.body()).asJsonArray

        val employerPayslips = mutableListOf<SystemEIavPayslip>()

        for (jsonElement in jsonArray) {
            val jsonObject = jsonElement.asJsonObject

            val employerId: Int = jsonObject.get("employerId").asInt
            val personalNumber: String = jsonObject.get("personalNumber").asString
            val hasCert: Boolean = jsonObject.get("hasCert").asBoolean
            val certId: Int = jsonObject.get("certId").asInt
            val year: Int = jsonObject.get("year").asInt
            val month: Int = jsonObject.get("month").asInt
            val income: Int = jsonObject.get("income").asInt

            val payslip = SystemEIavPayslip(employerId, personalNumber, hasCert, certId, year, month, income)
            employerPayslips.add(payslip)
            println("Calculated taxes for person: ${payslip.personalNumber}")
        }
        blockingQueue.add(employerPayslips)
        println("Calculated taxes for all employees at employer ${employerPayslips[0].employerId}")

        res.status(200)
        "ok"
    }
}

/**
 * Checks the payslip of a person.
 *
 * @param systemEIavPayslip The payslip of the person to check.
 */
private suspend fun controlPayslip(systemEIavPayslip: SystemEIavPayslip) {
    println("The taxes will now be calculated for the payslip for: ${systemEIavPayslip.personalNumber}")
}

/**
 * Checks if a specific certificate does exists in the IAVCertificate for a specific person or not.
 *
 * @param personalNumber The personal number of the person to check for.
 * @param certId The id of the certificate to check for.
 * @return True if the certificate exists or vice versa.
 */
private suspend fun checkCertificate(personalNumber: String, certId: Int): Boolean {
    val client = HttpClient(CIO)

    val startTime: Long = System.currentTimeMillis()
    val response: HttpResponse = client.request("http://localhost:4570/iav/checkCert?personalNumber=$personalNumber&certId=$certId") {
        headers {
            append("Accept", "application/json")
        }
        method = HttpMethod.Get
    }
    val stopTime: Long = System.currentTimeMillis()
    client.close()

    csvLogger.logOutgoingHttpRequest("GET", "Check if certificate is valid in IAVSystem",
        response.contentLength(), startTime, stopTime)

    return response.readText().toBoolean()
}

/**
 * Sends the personal payslip information over to system F.
 *
 * @param blockingQueue The queue to send from.
 */
private suspend fun sendInfoToSystemF(blockingQueue: LinkedBlockingQueue<List<SystemEIavPayslip>>) {
    val client = HttpClient(CIO)

    while (true) {
        val employerPayslips = blockingQueue.take()
        println("Sending in payslip with personalnumber: ${employerPayslips[0].personalNumber}")
        println("Sending in payslips for employer ${employerPayslips[0].employerId} to systemF")
        val startTime: Long = System.currentTimeMillis()

        while (true) {
            try {
                val response: HttpResponse = client.post("http://localhost:4569/systemF/postIncome") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    body = gson.toJson(employerPayslips)
                }
                val stopTime: Long = System.currentTimeMillis()

                csvLogger.logOutgoingHttpRequest("POST", "Sending payslips to system F",
                    response.contentLength(), startTime, stopTime)
                break
            } catch (exception: ConnectException) {
                println("Connect exception to systemF, trying again in 1 second")
                delay(1000)
                continue
            } catch (bindException: BindException) {
                println(bindException)
                delay(100)
                continue
            }
        }
    }
    client.close()
}