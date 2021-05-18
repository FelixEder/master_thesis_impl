package systems.iavSystem

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import systems.systemB.SystemBDBManager
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.iavControl.IAVControl
import systems.iavSystem.stickControl.StickControl
import spark.Spark.*
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger
import java.io.File

private val iavCsvLogger = CsvLogger("src/main/kotlin/csv/iavSystem/database.csv",
    "src/main/kotlin/csv/iavSystem/incomingHttp.csv",
    "src/main/kotlin/csv/iavSystem/outgoingHttp.csv")

private val systemBCsvLogger = CsvLogger("src/main/kotlin/csv/systemB/database.csv",
    "src/main/kotlin/csv/systemB/incomingHttp.csv",
    "src/main/kotlin/csv/systemB/outgoingHttp.csv")

private val stickControlLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/iavSystem/stickControlLog.csv",
    "personalNumber", "time (ms)")

/**
 * Main file of the project, here everything is started.
 *
 * @author Felix Eder
 * @date 2021-04-02
 */
fun main() {
    val psqlDbManager = PSQLDBManager(iavCsvLogger)
    val systemBDbManager = SystemBDBManager(systemBCsvLogger)
    val iavControl = IAVControl(psqlDbManager, systemBDbManager, iavCsvLogger)
    val stickControl = StickControl(psqlDbManager, iavCsvLogger)

    setUpHttpServer(psqlDbManager, stickControl)

    while (true) {
        when(readLine()) {
            "iav" -> {
                println("Starting IAVControl process")
                GlobalScope.launch {
                    val startTime = System.currentTimeMillis()
                    iavControl.startIAVProcess()
                    val stringSummary = iavCsvLogger.getSummarizedString()

                    File("src/main/kotlin/csv/summaryScenario1.txt").writeText(
                        "IAVControl start time: $startTime ms\n$stringSummary"
                    )
                }
            }
            "stick" -> {
                GlobalScope.launch {
                    stickControl.startStickControlProcess(2020, 1)
                }
            }

            "log" -> {
                iavCsvLogger.writeSummaryToConsole()
            }

            "close" -> {
                iavCsvLogger.close()
            }
            else ->
                println("Unknown input")
        }
    }
}

/**
 * Sets up a simple HTTP server that checks if a certificate exists in the registry.
 *
 * @param psqldbManager The database manager for the IAV registry.
 */
private fun setUpHttpServer(psqldbManager: PSQLDBManager, stickControl: StickControl) {
    port(4570)

    get("/iav/checkCert") { req, res ->
        iavCsvLogger.logIncomingHttpRequest("GET", "Request to check certificate authenticity",
                req.contentLength())

        val personalNumber: String = req.queryParams("personalNumber").toString()
        val certId: Int = req.queryParams("certId").toInt()
        res.status(200)
        psqldbManager.doesCertificateExist(personalNumber, certId)
    }

    get("iav/monhlyIncomesDone") { req, res ->
        iavCsvLogger.logIncomingHttpRequest("GET",
            "Request from systemF that monthly incomes are reported", req.contentLength())

        val year: Int = req.queryParams("year").toInt()
        val month: Int = req.queryParams("month").toInt()

        GlobalScope.launch {
            stickControl.startStickControlProcess(year, month)
        }

        res.status(200)
        "ok"
    }
}
