package systems.iavSystem.iavControl

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.systemB.SystemBDBManager
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.database.dataclasses.IAVCertificate
import systems.iavSystem.database.dataclasses.PackageToSystemC
import systems.systemA.dataclasses.PersonalIncome
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger
import java.io.InputStream
import java.io.InputStreamReader
import java.net.ConnectException
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDate


/**
 * Service that does the IAV control for a specific person.
 *
 * @author Felix Eder
 * @date 2021-04-03
 */
class IAVControl(private val psqlDbManager: PSQLDBManager, private val systemBDBManager: SystemBDBManager,
                 private val csvLogger: CsvLogger) {
    private val gson = Gson()
    private var runCounter = 0

    /**
     * Starts the IAVControl process
     */
    suspend fun startIAVProcess() {
        runCounter++
        val batchId = makeBatchRequest()
        readFromBatchFile(batchId)
    }

    /**
     * Calls the systemA service in order to get a batch with information.
     */
    private suspend fun makeBatchRequest(): Int {
        val client = HttpClient(CIO)
        var parsedResponse: Int

        val systemADowntimeLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/systemA/downtime$runCounter.csv",
            "description", "time (ms)")
        var systemADownTimeStart = 0L
        val startTime: Long = System.currentTimeMillis()

        while (true) {
            try {
                val response: HttpResponse = client.request("http://localhost:4568/systemA?maxSalary=100000&maxCapital=20000&incomeYears=5") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Get
                }
                val stopTime: Long = System.currentTimeMillis()

                if (systemADownTimeStart != 0L) {
                    systemADowntimeLogger.log("System A system has been down", stopTime - systemADownTimeStart)
                    systemADowntimeLogger.close()
                }
                client.close()

                parsedResponse = response.readText().toInt()

                csvLogger.logOutgoingHttpRequest("GET", "Logging outgoing system A batch request",
                    response.contentLength(), startTime, stopTime)
                break
            } catch (exception: ConnectException) {
                if (systemADownTimeStart == 0L)
                    systemADownTimeStart = System.currentTimeMillis()
                println("Connect exception to systemA, trying again in 1 second")
                delay(1000)
                continue
            }
        }

        return parsedResponse
    }

    private fun readFromBatchFile(batchId: Int) {
        val inputStream: InputStream = Files.newInputStream(Paths.get("src/main/kotlin/services/batches/systemABatch_$batchId.json"))
        val reader = JsonReader(InputStreamReader(inputStream))

        reader.beginArray()
        while (reader.hasNext()) {

            val startTime = System.currentTimeMillis()

            val personalIncome: PersonalIncome = gson.fromJson(reader, PersonalIncome::class.java)

            val systemBInfo = Pair(!reader.hasNext(), startTime)

            makeIAVCheck(personalIncome.personalNumber, systemBInfo)
        }
        reader.endArray()
    }

    /**
     * Check if a person is eligible for a IAVCertificate.
     *
     * @param personalNumber The personal number of the person to check for.
     */
    private fun makeIAVCheck(personalNumber: String, systemBInfo: Pair<Boolean, Long>) {
        val person = systemBDBManager.getPersonByPersonalNumber(personalNumber)

        if (person == null || checkAlreadyRegistered(personalNumber)) {
            println("Person is not eligible or already has a valid certificate.")
            return
        }

        createIAVCertificate(personalNumber, systemBInfo)
    }

    /**
     * Creates an IAVCertificate and inserts it in the database.
     *
     * @param personalNumber The personal number to create the certificate for.
     */
    private fun createIAVCertificate(personalNumber: String, systemCInfo: Pair<Boolean, Long>) {
        val currentDate = LocalDate.now()
        val expirationDate = currentDate.plusYears(6)

        val certId = psqlDbManager.insertCertificate(personalNumber, currentDate.toString(), expirationDate.toString())

        val iavCertificate = IAVCertificate(certId, personalNumber, currentDate.toString(), expirationDate.toString())

        val packageTosystemC = PackageToSystemC(iavCertificate, systemCInfo.first, systemCInfo.second)

        GlobalScope.launch {
            sendCertificateToSystemC(packageTosystemC)
        }
    }

    /**
     * Sends a newly created certificate to the systemC postal service in  order to send it out to the
     * person.
     *
     * @param iavCertificate The newly created IAVCertificate.
     */
    private suspend fun sendCertificateToSystemC(packageToSystemC: PackageToSystemC) {
        val client = HttpClient(CIO)

        val startTime: Long = System.currentTimeMillis()
        val response: HttpResponse = client.post("http://localhost:4572/systemC/certGranted") {
            headers {
                append("Accept", "application/json")
            }
            method = HttpMethod.Post
            contentType(ContentType.Application.Json)
            body = gson.toJson(packageToSystemC)
        }
        val stopTime: Long = System.currentTimeMillis()

        client.close()

        csvLogger.logOutgoingHttpRequest("POST", "Logging Post request to systemC", response.contentLength(), startTime, stopTime)

        println("Person ${packageToSystemC.iavCertificate.personalNumber} has been issued a IAVCertificate")
    }

    /**
     * Check if a person already holds a valid IAV certificate and is thus in the IAV registry.
     *
     * @param personalNumber The personal number of the person to check registry for.
     * @return True if a person is already in the IAV registry and false if not.
     */
    private fun checkAlreadyRegistered(personalNumber: String): Boolean {
        return psqlDbManager.getCertificate(personalNumber) != null
    }
}