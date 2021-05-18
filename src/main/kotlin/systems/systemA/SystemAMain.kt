package systems.systemA

import spark.Spark.*
import util.logging.CsvLogger

/**
 * A simple web service that reads basic personal information from a file and sets up a simple
 * web api in order to get the information.
 *
 * @author Felix Eder
 * @date 2021-04-12
 */
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemA/database.csv",
    "src/main/kotlin/csv/systemA/incomingHttp.csv",
    "src/main/kotlin/csv/systemA/outgoingHttp.csv")
private val SYSTEM_A_LOGIC: SystemALogic = SystemALogic(csvLogger)

/**
 * Main function that calls other functions to set up the web service.
 */
fun main() {
    setUpHttpServer()
}

/**
 * Sets up a simple HTTP server that listens for GET requests and sends over the user data.
 */
private fun setUpHttpServer() {
    port(4568)
    get("systemA") { req, res ->
        csvLogger.logIncomingHttpRequest("GET","Incoming batch request to systemA", req.contentLength())

        val maxSalary: Int = req.queryParams("maxSalary").toInt()
        val maxCapital: Int = req.queryParams("maxCapital").toInt()
        val incomeYears: Int = req.queryParams("incomeYears").toInt()

        val id = SYSTEM_A_LOGIC.createIncomeBatch(maxSalary, maxCapital, incomeYears)

        if (id == -1) {
            res.status(400)
        }
        else
            res.status(200)
            id
    }
}