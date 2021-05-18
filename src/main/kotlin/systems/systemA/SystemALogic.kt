package systems.systemA

import com.google.gson.Gson
import com.google.gson.stream.JsonWriter
import systems.systemA.dataclasses.FiveYearIncome
import util.logging.CsvLogger
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Class that handles the underlying logic for the systemA service.
 *
 * @author Felix Eder
 * @date 2021-04.14
 */
class SystemALogic(csvLogger: CsvLogger) {
    private val gson = Gson()
    private var id: Int = 0
    private val systemADBManager: SystemADBManager = SystemADBManager(csvLogger)

    /**
     * Creates a batch file based on the given parameters.
     *
     * @param maxSalary The maximum salary income for a person.
     * @param maxCapital The maximum capital income for a person.
     * @return The id of the batch file
     */
    fun createIncomeBatch(maxSalary: Int, maxCapital: Int, incomeYears: Int): Int {
        val fiveYearIncomes = systemADBManager.getAllValidPersons(maxSalary, maxCapital)

        return writeDataToFile(fiveYearIncomes)
    }

    /**
     * Creates a batch file for a query and returns the id of the batch file.
     *
     * @return The id of the batch file.
     */
    private fun writeDataToFile(personalIncomes: MutableList<FiveYearIncome>): Int {
        val oldId = id

        val outputStream: OutputStream = Files.newOutputStream(Paths.get("src/main/kotlin/services/batches/systemABatch_$id.json"))
        val writer = JsonWriter(OutputStreamWriter(outputStream))

        writer.beginArray()
        for (personalIncome in personalIncomes) {
            gson.toJson(personalIncome, FiveYearIncome::class.java, writer)
        }
        writer.endArray()
        writer.close()

        id++
        return oldId
    }

    /**
     * Checks if a person hasn't had a higher income over a certain set of years (can check both salary and capital).
     *
     * @param maxIncome The maximum income the person can make within the given time (inclusive).
     * @param incomeYears The number of years to go back in history to check the income (starting with the latest reported year).
     * @param incomeHistory A list of the recorded yearly income, with the latest year in the last location.
     *
     * @return True if the person hasn't made more than the limit and false if vice versa.
     */
    fun checkIncome(maxIncome: Int, incomeYears: Int, incomeHistory: List<Int>): Boolean {
        var salaryIncome = 0
        var localIncomeYears = incomeYears

        if (incomeHistory.isEmpty()) return true //If no salary history, the person approved automatically.

        if (incomeHistory.size < incomeYears)
            localIncomeYears = incomeHistory.size

        val salaryIndex = incomeHistory.size - 1

        for (i in salaryIndex downTo salaryIndex - localIncomeYears) {
            if (i < 0) break
            salaryIncome += incomeHistory[i]
        }

        if (salaryIncome > maxIncome) return false //Salary income greater than 100000 in the last 5 years

        return true
    }
}
