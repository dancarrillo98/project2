package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DataApp{
    def main(args: Array[String]): Unit = {
        //Replace the localPath variable with your preferred destination
        //Hortonworks SandBox
        val localPath = "file:///home/maria_dev/project2data/"
        //Begench S3
        //val localPath = "s3://alchemy-project-2/data/"
        //My Test S3
        //val localPath = "s3://project2datanalyzer/data/"
        
        try{
            val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("DataAnalyzer")
            .getOrCreate()
            val sc = spark.sparkContext

            spark.sparkContext.setLogLevel("ERROR")
            import spark.implicits._

            val df = spark.read
                .option("header", true)
                .option("inferSchema", true)
                .csv(localPath + "real_estate_db.csv")

            while (true){
                printOptions()
                val userAction = performQuery(spark, df, localPath)
                if (userAction == "11"){
                    return
                }
            }
            } finally {
                println("Exiting program.")
            }
    }

    //Query the results for Question 4
    def getQuery4(df: DataFrame, localPath: String): Unit = {
        //Query the results by state
        //df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("family_mean")).sort(asc("state")).show(52, false)
        df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("family_mean")).sort(asc("state")).coalesce(1).write.mode("overwrite").csv(localPath + "Q4ByState")

        //Query the results by city
        //df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("family_mean")).sort(asc("city")).show(10000, false)
        df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("family_mean")).sort(asc("city")).coalesce(1).write.mode("overwrite").csv(localPath + "Q4ByCity")
    }

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

    //Query the results for Question 8
    def getQuery8(df: DataFrame, localPath: String): Unit = {
        //Query the results by state
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).show(52, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByState")

        //Query the results by city
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).show(10000, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByCity")
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

    def printOptions(): Unit = {
        println("Welcome to your big data analyzer application.")
        println("[1] Q1")
        println("[2] Q2")
        println("[3] Q3")
        println("[4] Q4")
        println("[5] Q5")
        println("[6] Q6")
        println("[7] Q7")
        println("[8] Q8")
        println("[9] Q9")
        println("[10] Q10")
        println("[11] Exit")
    }

    def getUserAction(): String = {
        val userAction = readLine("Choose an action: ")
        println("\n")
        userAction
    }

    def performQuery(spark: SparkSession, df: DataFrame, localPath: String): String = {
        val userAction = getUserAction()
        userAction match {
            case "1" => // Q1
            case "2" => // Q2
            case "3" => // Q3
            case "4" => {
                println("Performing query for Question 4 ...")
                getQuery4(df, localPath)
            }
            case "5" => // Q5
            case "6" => // Q6
            case "7" => {
                println("Performing query for Question 7 ...")
                getQuery7(spark, df, localPath)
            }
            case "8" => {
                 println("Performing query for Question 8 ...")
                getQuery8(df, localPath)
            }
            case "9" => {
                println("Performing query for Question 9 ...")
                getQuery9(spark, df, localPath)
            }
            case "10" => // Q10
            case "11" => println("Have a nice day!")
            case _ => println("Not a valid option, please try again.")
        }
        userAction
    }
}