package systems.iavSystem.stickControl

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.delay
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.database.dataclasses.IAVCertificate
import systems.iavSystem.database.dataclasses.IAVIncomeReq
import systems.iavSystem.stickControl.dataclasses.StickSet
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger
import java.net.ConnectException
import kotlin.random.Random

/**
 * Service that does the stick control for all valid IAV certificates, should be run monthly.
 *
 * @author Felix Eder
 * @date 2021-04-19
 */
class StickControl (private val psqlDbManager: PSQLDBManager, private val csvLogger: CsvLogger) {
    private val gson = Gson()
    private val random = Random(123456789)
    private var runCounter = 0
    private val systemFDowntimeLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/iavSystem/systemFDowntime.csv",
        "description", "time (ms)")

    /**
     * Starts the complete stick control process, all the relevant information is fetched and the
     * individual checks are performed.
     *
     * @param year The year to perform the stick control for.
     * @param month The month to perform the stick control for.
     */
    suspend fun startStickControlProcess(year: Int, month: Int) {
        runCounter++
        val stickControlLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/iavSystem/stickControlPerson$runCounter.csv",
            "personalNumber", "time (ms)")

        val certIds = psqlDbManager.getAllCertIds()

        for (certId in certIds) {
            val startTime = System.currentTimeMillis()
            val cert: IAVCertificate = psqlDbManager.getCertificate(certId) as IAVCertificate
            val monthlyIncome = getIncomeFromSystemF(cert.personalNumber, year, month)
            val incomeInfo = IAVIncomeReq(cert.personalNumber, monthlyIncome, certId)

            startSingleStickControl(incomeInfo)
            val stopTime = System.currentTimeMillis()
            stickControlLogger.log(cert.personalNumber, stopTime - startTime)
        }
    }

    /**
     * Performs the stick control process for a single income, performs logic checks and
     * then updates the values correctly. Calls private helper methods to help with performing the
     * task.
     */
    private suspend fun startSingleStickControl(incomeInfo: IAVIncomeReq) {
        val stickSet = psqlDbManager.getStickSet(incomeInfo.personalNumber)
        val updatedStickSet = updateSticks(incomeInfo.personalNumber, stickSet, incomeInfo.income)
        if (controlStickSet(updatedStickSet))
            updateStickInformation(updatedStickSet)
        else
            unregisterCertificate(incomeInfo.personalNumber)
    }

    /**
     * Updates the stickSet for a person based on their new monthly income. If the person has no
     * stickSet yet, a new one is created and stored in the database.
     *
     * @param personalNumber The personal number of the person to update sticks for.
     * @param additionalInfo A pair with a nullable stickSet as well as this months income as an integer.
     *
     * @return The updated stickSet.
     */
    private fun updateSticks(personalNumber: String, stickSet: StickSet?, income: Int): StickSet {
        var localStickSet = stickSet
        if (stickSet == null) { //Person has no stick set, create one and store it in db.
            localStickSet = StickSet(personalNumber, 0, 0)
            psqlDbManager.insertStickSet(localStickSet)
        }

        if (income >= 5000)
            (localStickSet as StickSet).sticks5K++

        if (income >= 28000)
            (localStickSet as StickSet).sticks28K++

        return (localStickSet as StickSet)
    }

    /**
     * Controls if a given stickSet has gone over the limits of the IAV. Then returns a boolean
     * representing the result.
     *
     * @param stickSet The stickSet to control.
     * @return True if the stickSet is still under the limit and the person can keep the certificate
     * and false if vice versa.
     */
    private fun controlStickSet(stickSet: StickSet): Boolean {
        return stickSet.sticks28K < 3 && stickSet.sticks5K < 24
    }

    /**
     * Updates the stick set in the database.
     */
    private fun updateStickInformation(stickSet: StickSet) {
        psqlDbManager.updateSpecificStickSet(stickSet)
        println("Updated the stick set for: ${stickSet.personalNumber}")
    }

    /**
     * Gets the income for a specific person from systemF.
     *
     * @param personalNumber The personal number of the person to get the person for.
     * @param year The year to get the income from.
     * @param month The month to get the income from.
     */
    private suspend fun getIncomeFromSystemF(personalNumber: String, year: Int, month: Int): Int {
        val client = HttpClient(CIO)

        var systemFDowntimeStart = 0L
        val startTime = System.currentTimeMillis()

        while (true) {
            try {
                if (random.nextInt(0, 100) < 3)
                    throw ConnectException()
                val response: HttpResponse = client.request(
                    "http://localhost:4569/systemF/income?personalNumber=$personalNumber&year=$year&month=$month") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Get
                }
                val stopTime: Long = System.currentTimeMillis()

                if (systemFDowntimeStart != 0L) {
                    systemFDowntimeLogger.log("systemF system has been down", stopTime - systemFDowntimeStart)
                }
                client.close()

                csvLogger.logOutgoingHttpRequest("GET", "Get income from systemF", response.contentLength(), startTime, stopTime)
                return response.readText().toInt()

            } catch (exception: ConnectException) {
                if (systemFDowntimeStart == 0L)
                    systemFDowntimeStart = System.currentTimeMillis()
                println("Connect exception to systemF, trying again in 1 second")
                delay(1000)
                continue
            }
        }
    }

    /**
     * Removes the StickSet and IAVCertificate from the database for a specific person and
     * sends a request to the postalService to send out the information to the affected person.
     */
    private suspend fun unregisterCertificate(personalNumber: String) {
        psqlDbManager.removeStickSet(personalNumber)

        val iavCertificate = psqlDbManager.getCertificate(personalNumber) as IAVCertificate
        psqlDbManager.removeCertificate(iavCertificate.id)

        val client = HttpClient(CIO)

        val startTime: Long = System.currentTimeMillis()
        val response: HttpResponse = client.post("http://localhost:4572/systemC/certUnregistered") {
            headers {
                append("Accept", "application/json")
            }
            method = HttpMethod.Post
            contentType(ContentType.Application.Json)
            body = gson.toJson(iavCertificate)
        }
        val stopTime: Long = System.currentTimeMillis()
        client.close()

        csvLogger.logOutgoingHttpRequest("POST",
            "Send request to systemC to unregister certificate", response.contentLength(),
            startTime, stopTime)

        println("Person $iavCertificate.personalNumber has been unregistrered from IAVRegistry")
    }
}