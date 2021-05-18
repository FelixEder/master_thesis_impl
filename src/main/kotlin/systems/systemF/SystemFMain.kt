package systems.systemF

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import systems.systemF.dataclasses.IncomeReq
import systems.systemF.dataclasses.MonthlyIncome
import spark.Spark.*
import util.logging.CsvLogger
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Simple web service that handles connection with the monthly income database and sets up a
 * REST api in order to send out the information.
 *
 * @author Felix Eder
 * @date 2021-04-17
 */

private val csvLogger = CsvLogger("src/main/kotlin/csv/systemF/database.csv",
    "src/main/kotlin/csv/systemF/incomingHttp.csv",
    "src/main/kotlin/csv/systemF/outgoingHttp.csv")
private val systemFDBManager = SystemFDBManager(csvLogger)
private val gson = Gson()
private const val numberOfEmployers = 10000 //Used for logging purposes

/**
 * Main function that calls other functions to set up the web service.
 */
fun main(args: Array<String>) {
    val year: Int = args[0].toInt()
    val month: Int = args[1].toInt()
    val concurrentQueue = LinkedBlockingQueue<List<MonthlyIncome>>()

    setUpHttpServer(year, month, concurrentQueue)
    GlobalScope.launch {
        handleIncomeQueue(concurrentQueue, year, month)
    }
}

/**
 * Sets up a simple HTTP server that listens for GET requests and sends over monthly income data.
 */
private fun setUpHttpServer(systemFYear: Int, systemFMonth: Int, concurrentQueue: LinkedBlockingQueue<List<MonthlyIncome>>) {
    //val concurrentTransactions = AtomicInteger()
    //concurrentTransactions.set(0)
    port(4569)

    post("systemF/postIncome") { req, res ->
        csvLogger.logIncomingHttpRequest("POST",
            "Incoming request with income data from system D", req.contentLength())

        val incomes: List<MonthlyIncome> = gson.fromJson(req.body(), Array<MonthlyIncome>::class.java).toList()

        concurrentQueue.add(incomes)
        res.status(200)
        "ok"
    }

    get("systemF/income") { req, res ->
        csvLogger.logIncomingHttpRequest("GET",
            "Request to get monthly income from systemF", req.contentLength())

        val personalNumber: String = req.queryParams("personalNumber").toString()
        val year: Int = req.queryParams("year").toInt()
        val month: Int = req.queryParams("month").toInt()

        systemFDBManager.getMonthlyIncome(personalNumber, year, month)
    }
}

private suspend fun handleIncomeQueue(queue: LinkedBlockingQueue<List<MonthlyIncome>>, systemFYear: Int, systemFMonth: Int) {
    val payslipsReceived = AtomicInteger()
    payslipsReceived.set(0)

    while (true) {
        val incomes = queue.take()
        val payslips = payslipsReceived.incrementAndGet()
        systemFDBManager.bulkInsertMonthlyIncome(incomes)
        println("Inserted monthly incomes for employer: ${incomes[0].employerId}")

        if (payslips >= numberOfEmployers) {
            println("Have gotten all the payslips this salary period")
            val stopTime = System.currentTimeMillis()

            val stringBuilder = StringBuilder()

            stringBuilder.appendLine("Stop Scenario 3a from employer")
            stringBuilder.appendLine("Stop time (ms): $stopTime")

            File("src/main/kotlin/csv/summaryScenario3aEnd.txt").writeText(stringBuilder.toString())

            /*
            Skip in this case
            GlobalScope.launch {
                sendIncomesDoneToIav(systemFYear, systemFMonth)
            }
             */
            payslipsReceived.set(0)
        }
    }
}

/**
 * Sends a request to the IAVSystem that all the payslips have been collected for this time
 *
 * @param systemFYear The year of the current payslip period.
 * @param systemFMonth The month of the current payslip period.
 */
private suspend fun sendIncomesDoneToIav(systemFYear: Int, systemFMonth: Int) {
    val client = HttpClient(CIO)

    val startTime = System.currentTimeMillis()
    val response: HttpResponse = client.request("http://localhost:4570/iav/monhlyIncomesDone?year=$systemFYear&month=$systemFMonth") {
        headers {
            append("Accept", "application/json")
        }
        method = HttpMethod.Get
    }
    val stopTime = System.currentTimeMillis()
    client.close()

    csvLogger.logOutgoingHttpRequest("GET",
        "Send request to Iav that all incomes have come in.",
        response.contentLength(), startTime, stopTime)
}

/**
 * Sends over monthly income information to IAVSystem
 * DEPRECATED
 */
private suspend fun sendIncomeToIAV(incomeReq: IncomeReq) {
    val client = HttpClient(CIO)

    val startTime: Long = System.currentTimeMillis()
    val response: HttpResponse = client.post("http://localhost:4570/iav/postIncome") {
        headers {
            append("Accept", "application/json")
        }
        method = HttpMethod.Post
        contentType(ContentType.Application.Json)
        body = gson.toJson(incomeReq)
    }
    val stopTime: Long = System.currentTimeMillis()
    client.close()

    csvLogger.logOutgoingHttpRequest("POST", "Sending income to iav system",
        response.contentLength(), startTime, stopTime)

    println("Sent over monthly income to iav system")
}