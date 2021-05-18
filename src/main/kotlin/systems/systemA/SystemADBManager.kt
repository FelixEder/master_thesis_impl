package systems.systemA

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import systems.systemA.dataclasses.FiveYearIncome
import util.logging.CsvLogger

/**
 * Class that sets up and manages the database associated with the systemA service.
 *
 * @author Felix Eder
 * @date 2021-04-15
 */
class SystemADBManager(private val csvLogger: CsvLogger) {
    private val db = Database.connect("jdbc:postgresql://localhost/PersonalIncome?rewriteBatchedInserts=true", driver = "org.postgresql.Driver",
        user = "exjobb", password = "1234")

    object FiveYearPersonalIncomes : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50).uniqueIndex()
        val salaryIncome: Column<Int> = integer("salaryIncome")
        val capitalIncome: Column<Int> = integer("capitalIncome")
    }

    class FiveYearPersonalIncome(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<FiveYearPersonalIncome>(FiveYearPersonalIncomes)
        var personalNumber by FiveYearPersonalIncomes.personalNumber
        var salaryIncome by FiveYearPersonalIncomes.salaryIncome
        var capitalIncome by FiveYearPersonalIncomes.capitalIncome
    }

    init {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(FiveYearPersonalIncomes)
        }
    }

    /**
     * Inserts a batch of yearly income objects into the database.
     *
     * @param fiveYearIncomes The list of yearly incomes to insert.
     */
    fun batchInsertYearlyIncome(fiveYearIncomes: List<FiveYearIncome>) {
        transaction(db) {
            addLogger(StdOutSqlLogger)

            FiveYearPersonalIncomes.batchInsert(fiveYearIncomes,
                ignore = false,
                shouldReturnGeneratedValues = false
            ) {
                this[FiveYearPersonalIncomes.personalNumber] = it.personalNumber
                this[FiveYearPersonalIncomes.salaryIncome] = it.salaryIncome
                this[FiveYearPersonalIncomes.capitalIncome] = it.capitalIncome
            }
        }
    }

    /**
     * Gets all the persons from the database that fill a specific mold.
     * @param maxSalary The maximum salary amount for the person.
     * @param maxCapital The maximum capital amount for the person.
     *
     * @return a list of all the specified persons.
     */
    fun getAllValidPersons(maxSalary: Int, maxCapital: Int): MutableList<FiveYearIncome> {
        val personalNumbers = mutableListOf<FiveYearIncome>()
        transaction(db) {
            addLogger(StdOutSqlLogger)
            FiveYearPersonalIncome.find { FiveYearPersonalIncomes.salaryIncome lessEq  maxSalary and(FiveYearPersonalIncomes.capitalIncome lessEq maxCapital) }
                .forEach { personalNumbers.add(FiveYearIncome(it.personalNumber, it.salaryIncome, it.capitalIncome)) }
        }
        csvLogger.logDatabaseAccess("READ", "Get personal numbers from database")

        return personalNumbers
    }

    /**
     * Gets a personal income summary for a given personal number and a given amount of years.
     *
     * @param personalNumber The personal number of the person to get income history from.
     * @param incomeYears The number of years back to fetch history by.
     * @return The personal income object of the person.
    fun getIncomeByPersonalNumber(personalNumber: String, incomeYears: Int): PersonalIncome {
        val personalIncome = PersonalIncome(personalNumber, mutableListOf(), mutableListOf())

        transaction(db) {
            addLogger(StdOutSqlLogger)

            val resultList = FiveYearPersonalIncome.find { FiveYearPersonalIncomes.personalNumber eq personalNumber}.
                    orderBy(FiveYearPersonalIncomes.year to SortOrder.ASC).limit(incomeYears).toList()

            for (yearlyIncome in resultList) {
                personalIncome.yearlyCapitalIncome.add(yearlyIncome.capitalIncome)
                personalIncome.yearlySalaryIncome.add(yearlyIncome.salaryIncome)
            }
        }
        csvLogger.logDatabaseAccess("READ", "Get yearly income from database")

        return personalIncome
    }
     */
}