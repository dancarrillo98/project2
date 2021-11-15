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
        // HDFS path
        //val localPath = "/user/maria_dev/project2/"
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

    //Query the results for Question 5: "Do high school degrees correlate with less debt?"
    def getQuery5(spark: SparkSession, df: DataFrame, localPath: String): Unit = {

        // Create DF with relevant fields
        val newDF = df.select("city", "state", "debt", "hs_degree")
        newDF.createOrReplaceTempView("Debt_HSDegree")
        val dfTable = spark.sql("SELECT state AS State, ROUND(AVG(CAST(debt AS decimal)*100), 2) AS Debt_Percentage, ROUND(AVG(CAST(hs_degree AS decimal)*100), 2) AS High_School_Degree_Percentage FROM Debt_HSDegree GROUP BY State ORDER BY Debt_Percentage DESC")

        dfTable.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(localPath + "Q5ByState")

        // Calculate Correlation Coefficient
        dfTable.createOrReplaceTempView("Debt_HSDegree_Adjusted")
        val dfCoef = spark.sql("SELECT ROUND(((AVG(Debt_Percentage*High_School_Degree_Percentage)-(AVG(Debt_Percentage)*AVG(High_School_Degree_Percentage))) / (STD(Debt_Percentage)*STD(High_School_Degree_Percentage))), 2) AS q5_coefficient FROM Debt_HSDegree_Adjusted")
        
        dfCoef.write
            .option("header", "true")
            .csv(localPath + "Q5ByState_Coefficient")

    }


    //Query the results for Question 6: "Does general population age correlate with less debt?"
    def getQuery6(spark: SparkSession, df: DataFrame, localPath: String): Unit = {

        // Create DF with relevant fields
        val newDF = df.select("city", "state", "male_age_mean", "female_age_mean", "debt")
        newDF.createOrReplaceTempView("Age_Debt")
        val dfTable = spark.sql("SELECT state AS State, ROUND(AVG(CAST(debt AS decimal)*100), 2) AS Debt_Percentage, ROUND(AVG((CAST(male_age_mean AS decimal) + CAST(female_age_mean as decimal))/2), 2) AS Average_Age FROM Age_Debt GROUP BY State ORDER BY Debt_Percentage DESC")

        dfTable.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(localPath + "Q6ByState")

        // Calculate Correlation Coefficient
        dfTable.createOrReplaceTempView("Age_Debt_Adjusted")
        val dfCoef = spark.sql("SELECT ROUND(((AVG(Debt_Percentage*Average_Age)-(AVG(Debt_Percentage)*AVG(Average_Age))) / (STD(Debt_Percentage)*STD(Average_Age))), 2) AS q6_coefficient FROM Age_Debt_Adjusted")
        
        dfCoef.write
            .option("header", "true")
            .csv(localPath + "Q6ByState_Coefficient")

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



    //Query the results for Question 10: "Does a larger population mean higher rent?"
    def getQuery10(spark: SparkSession, df: DataFrame, localPath: String): Unit = {

        // Create DF with relevant fields
        val newDF = df.select("city", "state", "pop", "rent_mean")
        newDF.createOrReplaceTempView("Rent_Pop")
        val dfTable = spark.sql("SELECT city AS City, state AS State, SUM(CAST(pop AS decimal)) AS Population, ROUND(AVG(CAST(rent_mean AS decimal)), 2) AS AverageRent FROM Rent_Pop GROUP BY city, state ORDER BY Population DESC")

        dfTable.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(localPath + "Q10ByCity")

        // Calculate Correlation Coefficient
        dfTable.createOrReplaceTempView("Rent_Pop_Adjusted")
        val dfCoef = spark.sql("SELECT ROUND(((AVG(Population*AverageRent)-(AVG(Population)*AVG(AverageRent))) / (STD(Population)*STD(AverageRent))), 2) AS q10_coefficient FROM Rent_Pop_Adjusted")
        
        dfCoef.write
            .option("header", "true")
            .csv(localPath + "Q10ByCity_Coefficient")

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
            case "5" => {
                println("Performing query for Question 4 ...")
                getQuery5(spark, df, localPath)
            }
            case "6" => {
                println("Performing query for Question 4 ...")
                getQuery6(spark, df, localPath)
            }
            case "7" => // Q7
            case "8" => {
                 println("Performing query for Question 8 ...")
                getQuery8(df, localPath)
            }
            case "9" => // Q9
            case "10" => {
                println("Performing query for Question 4 ...")
                getQuery10(spark, df, localPath)
            }
            case "11" => println("Have a nice day!")
            case _ => println("Not a valid option, please try again.")
        }
        userAction
    }
}

