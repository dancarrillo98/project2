package project2

// Spark Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


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
        
        import spark.implicits._
        
        // Create SparkContext
        val sc = spark.sparkContext

        // Read csv data into a DataFrame
        val demographicsData = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(hdfsFilePath + "real_estate_db.csv")
        
        // Show schema and first 20 rows to verify
        println("Printing Schema...")
        demographicsData.printSchema()
       

        // Analyze data
        // Analysis 5.1: Create table for "Do high school degrees correlate with less debt?"
        val df5 = demographicsData.select("city", "state", "debt", "hs_degree")
        df5.createOrReplaceTempView("Debt_HSDegree")
        val df5Table = spark.sql("SELECT state AS State, ROUND(AVG(CAST(debt AS decimal)*100), 2) AS Debt_Percentage, ROUND(AVG(CAST(hs_degree AS decimal)*100), 2) AS High_School_Degree_Percentage FROM Debt_HSDegree GROUP BY State ORDER BY Debt_Percentage DESC")

        df5Table.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q5_Table")

        // Analysis 5.2: Calculate correlation coefficient for 5.1
        df5Table.createOrReplaceTempView("Debt_HSDegree_Adjusted")
        val df5Coef = spark.sql("SELECT ROUND(((AVG(Debt_Percentage*High_School_Degree_Percentage)-(AVG(Debt_Percentage)*AVG(High_School_Degree_Percentage))) / (STD(Debt_Percentage)*STD(High_School_Degree_Percentage))), 2) AS q5_coef_r FROM Debt_HSDegree_Adjusted")

        df5Coef.write
            .option("header", "true")
            .csv(hdfsFilePath + "q5_Coef")



        // Analysis 10.1: Create table for "Does a larger population mean higher rent?"
        val df10 = demographicsData.select("city", "state", "pop", "rent_mean")
        df10.createOrReplaceTempView("Rent_Pop")
        val df10Table = spark.sql("SELECT city AS City, state AS State, SUM(CAST(pop AS decimal)) AS Population, ROUND(AVG(CAST(rent_mean AS decimal)), 2) AS AverageRent FROM Rent_Pop GROUP BY city, state ORDER BY Population DESC")

        df10Table.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q10_Table")

        // Analysis 10.2: Calculate correlation coefficient r for 10.1
        df10Table.createOrReplaceTempView("Rent_Pop_Adjusted")
        val df10Coef = spark.sql("SELECT ROUND(((AVG(Population*AverageRent)-(AVG(Population)*AVG(AverageRent))) / (STD(Population)*STD(AverageRent))), 2) AS q10_coef_r FROM Rent_Pop_Adjusted")

        df10Coef.write
            .option("header", "true")
            .csv(hdfsFilePath + "q10_Coef")


    } // end main

} // end class