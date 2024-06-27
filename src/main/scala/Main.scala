import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max}

/**
 * Main object for running flight analysis using Spark.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Analysis")
      .master("local[*]")
      .getOrCreate()

    try {
      // Load flight data from CSV into a Dataset of FlightData objects
      val flightDataDS = DatasetUtils.loadFlightData(spark, "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\FlightAssessment\\flightData.csv")
      flightDataDS.show(false) // Display loaded flight data
      // Load passenger data from CSV into a Dataset of Passenger objects
      val passengerDS = DatasetUtils.loadPassengerData(spark, "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\FlightAssessment\\passengers.csv")
      passengerDS.show(false) // Display loaded passenger data

      // Perform analysis using various methods from Q1, Q2, Q3, Q4

      // Q1: Calculate number of flights for each month
      val numFlightsForEachMonthDS = Q1.getNumFlightsForEachMonth(flightDataDS)
      DatasetUtils.writeDataInCSV(numFlightsForEachMonthDS,
        "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\output\\numFlights")

      // Q2: Find top 100 frequent flyers
      val top100FrequentFlyersDS = Q2.top100FrequentFlyers(spark, flightDataDS, passengerDS)
      DatasetUtils.writeDataInCSV(top100FrequentFlyersDS,
        "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\output\\top100")

      // Q3: Determine longest non-UK trip for each passenger
      val longestNonUKTripDS = Q3.longestNonUKCount(flightDataDS)
      longestNonUKTripDS.show(false)
      DatasetUtils.writeDataInCSV(longestNonUKTripDS,
        "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\output\\nonuk")

      // Q4: Find pairs of passengers who flew together on multiple flights
      val flightsTogetherDS = Q4.findFlightsTogether(flightDataDS)
      flightsTogetherDS.show(false)
      DatasetUtils.writeDataInCSV(flightsTogetherDS.select("passenger1", "passenger2", "numFlightsTogether"),
        "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\output\\flightsTogether")

      // BonusQ: Find pairs of passengers who flew at least N times together with the range (from,to)
      // Get the date range from the dataset
      val dateRange = flightDataDS.agg(
        min("date").as("minDate"),
        max("date").as("maxDate")
      ).collect().head
      val from = dateRange.getAs[String]("minDate")
      val to = dateRange.getAs[String]("maxDate")
      val atLeastNTimes = 3 // This can be dynamically set based on the criteria
      val flownNFlightsTogether = BonusQ.flownTogether(atLeastNTimes, from, to, flightDataDS)
      flightsTogetherDS.show(false)
      DatasetUtils.writeDataInCSV(
        flownNFlightsTogether.select("passenger1", "passenger2", "numFlightsTogether", "from", "to"),
        "C:\\Users\\maitr\\OneDrive\\Desktop\\Quantexa\\output\\flightsTogetherFull")

    } finally {
      spark.stop()
    }
  }
}
