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
                val userAction = performQuery(df, localPath)
                if (userAction == "11"){
                    return
                }
            }
            } finally {
                println("Exiting program.")
            }
    }

    // Query: What are the ten wealthiest area codes in the US?
    def getQuery3(df: DataFrame, localPath: String):Unit = {
        //df.filter("family_mean != 'NaN'").groupBy("area_code").agg(sum("family_mean").as("total_avg_wealth")).orderBy(col("total_avg_wealth").desc).show(false)
        df.filter("family_mean != 'NaN'").groupBy("area_code").agg(sum("family_mean").as("total_avg_wealth")).orderBy(col("total_avg_wealth").desc).coalesce(1).write.mode("overwrite").csv(localPath + "Q3ByAreaCode")
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

    //Query the results for Question 8
    def getQuery8(df: DataFrame, localPath: String): Unit = {
        //Query the results by state
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).show(52, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByState")

        //Query the results by city
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).show(10000, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByCity")
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

    def performQuery(df: DataFrame, localPath: String): String = {
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
            case "7" => // Q7
            case "8" => {
                 println("Performing query for Question 8 ...")
                getQuery8(df, localPath)
            }
            case "9" => // Q9
            case "10" => // Q10
            case "11" => println("Have a nice day!")
            case _ => println("Not a valid option, please try again.")
        }
        userAction
    }
}