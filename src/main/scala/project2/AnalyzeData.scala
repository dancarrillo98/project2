package project2

// Spark Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame


object AnalyzeData {
    def main(args: Array[String]): Unit = {

        // Define path to csv file with data
        val hdfsFilePath = "/user/maria_dev/project2/"
        
        // Create SparkSession
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("project2")
            .getOrCreate()
    
        
        // Create SparkContext
        val sc = spark.sparkContext

        import spark.implicits._

        // Read csv data into a DataFrame
        val demographicsData = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(hdfsFilePath + "real_estate_db.csv")
        
        // Show schema and first 20 rows to verify
        println("Printing Schema...")
        demographicsData.printSchema()
       

        // Analyze data

    } // end main

    def getQuery7(spark: SparkSession, demographicsData: DataFrame, hdfsFilePath: String): Unit = {
        // Create table for "Do more high school degrees correlate with higher rent?"
        val df7 = demographicsData.select("city", "state", "pop", "rent_mean", "hs_degree")
        df7.createOrReplaceTempView("Rent_hs")
        val df7Table = spark.sql("SELECT city AS City, state AS State, SUM(CAST(pop AS int)) AS Population, ROUND(AVG(CAST(rent_mean AS int)), 2) AS AverageRent, CAST(hs_degree as float) AS degree FROM Rent_hs GROUP BY city, state ORDER BY degree DESC")

        df7Table.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q7_Table")

    }
    def getQuery9(spark: SparkSession, demographicsData: DataFrame, hdfsFilePath: String): Unit = {
        // Create table for "Are debt and home ownership correlated?"
        val df9 = demographicsData.select("city", "state", "debt", "home_equity")
        df9.createOrReplaceTempView("debt_home")
        val df9Table = spark.sql("SELECT city AS City, state AS State, CAST(debt AS float) AS Debt, CAST(home_equity AS float) AS HomeEquity FROM debt_home GROUP BY city, state ORDER BY Debt DESC")

        df9Table.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q9_Table")
    }
} // end class