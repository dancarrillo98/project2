package project2

// Spark Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


// Question import
import project2.Analysis._


object Main {

    // Define path to csv file with data
    val hdfsFilePath = "/user/maria_dev/project2/"
    val s3FilePaht = ""     // to implement later
    
    def main(args: Array[String]): Unit = {

        
        // Create SparkSession
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("project2")
            .getOrCreate()

        // Hide spark logging except if error
        spark.sparkContext.setLogLevel("ERROR")
        
        import spark.implicits._

        // Read csv data into a DataFrame
        val demographicsData = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(hdfsFilePath + "real_estate_db.csv")
        
        // CLI Menu goes here
        
        

        // Test analysis of q5
        addLine()
        loading("Analyzing question: " + q5Table.question)
        
        loading()
        val df = createDF(spark, demographicsData, q5Table.viewName, q5Table.sqlStatement, q5Table.columns)

        addLine()
        loading("Calculating correlation coefficient...")
        calculateCoef(spark, df, q5Coef.viewName, q5Coef.sqlStatement)


    } // end main

    
    // Create table for analysis question
    def createDF(spark: SparkSession, inputDF: DataFrame, viewName: String, sqlStatement: String, columns: Seq[String], save: Boolean = false, filename: String = ""): DataFrame = {
        val df = inputDF.select(columns.map(x => col(x)): _*)
        df.createOrReplaceTempView(viewName)
        val dfTable = spark.sql(sqlStatement)

        if (save) {
            dfTable.coalesce(1)
                .write
                .option("header", "true")
                .csv(hdfsFilePath + filename)
        } else {
            dfTable.show()
        }

        dfTable

    } // end createDF()


    // Calculate correlation coefficient r for a dataframe
    def calculateCoef(spark: SparkSession, inputDF: DataFrame, viewName: String, sqlStatement: String, save: Boolean = false, filename: String = ""): Unit = {
                
        inputDF.createOrReplaceTempView(viewName)
        val dfCoef = spark.sql(sqlStatement)

        // Save to a file or print
        if (save) {
            dfCoef.write
                .option("header", "true")
                .csv(hdfsFilePath + filename)
        } else {
            dfCoef.show()
        }
        
    } // end calculateCoef()


    def loading(text: String = "Loading..."): Unit = {
        println(text)
        Thread.sleep(1000)
    } // end loading()


    def addLine(): Unit = {
        println("--------------------------------------")
    }

} // end class