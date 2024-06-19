import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

case class FlightData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class Passenger(passengerId: Int, firstName: String, lastName: String)

/**
 * Utility methods for loading and manipulating Datasets in Spark.
 */
object DatasetUtils {
  /**
   * Loads flight data from a CSV file into a Dataset of FlightData objects.
   *
   * @param spark SparkSession instance for Spark SQL operations.
   * @param path  Path to the CSV file containing flight data.
   * @return Dataset of FlightData objects containing loaded flight data.
   */
  def loadFlightData(spark: SparkSession, path: String): Dataset[FlightData] = {
    import spark.implicits._
    val flightDataSchema = Encoders.product[FlightData].schema
    spark.read
      .option("header", "true")
      .schema(flightDataSchema)
      .csv(path)
      .as[FlightData]
  }
  /**
   * Loads passenger data from a CSV file into a Dataset of Passenger objects.
   *
   * @param spark SparkSession instance for Spark SQL operations.
   * @param path Path to the CSV file containing passenger data.
   * @return Dataset of Passenger objects containing loaded passenger data.
   */
  def loadPassengerData(spark: SparkSession, path: String): Dataset[Passenger] = {
    import spark.implicits._
    val passengerSchema = Encoders.product[Passenger].schema
    spark.read
      .option("header", "true")
      .schema(passengerSchema)
      .csv(path)
      .as[Passenger]
  }

  /**
   * Writes the contents of a Dataset to a CSV file.
   *
   * @param ds      Dataset to be written to CSV.
   * @param csvPath Path where the CSV file will be written.
   */
  def writeDataInCSV[T](ds: Dataset[T], csvPath: String): Unit = {
    ds.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimeter", ",").csv(csvPath)
  }
}

