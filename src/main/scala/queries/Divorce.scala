package queries

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.avg

object Divorce {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("DataAnalyzer")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._


    def main(args: Array[String]): Unit = {

        val local_path: String = "/home/hdoop/Projects/project2/"

        val df: DataFrame = spark.read
          .option("header", value = true)
          .option("inferSchema", value = true)
          .option("delimiter", ",")
          .csv(local_path + "src/main/resources/real_estate_db.csv")

        Query_1(df)

    }

    def Query_1(df: DataFrame): Unit = {

        // How does the ratio between male and
        // female correlate to divorce rates?
        
        // Population is not NaN, only 0
        // checking for male and female population
        // as NaN omits all the items for some reason
        val data: DataFrame = df
          .select(
            "state_ab", "city", "zip_code", "male_pop", "female_pop", "divorced"
          )

          .filter(df("divorced") =!= "NaN")


        // FIXME: city names are likely not unique b/w states
        val data_state: DataFrame = data
          .groupBy("state_ab")
          .agg(avg("male_pop"), avg("female_pop"), avg("divorced"))

        val data_city: DataFrame = data
          .groupBy("city")
          .agg(avg("male_pop"), avg("female_pop"), avg("divorced"))

        val data_place: DataFrame = data
          .groupBy("zip_code")
          .agg(avg("male_pop"), avg("female_pop"), avg("divorced"))

        data_state.show()
        data_city.show()
        data_place.show()

    }

    def Query_2(df: DataFrame): Unit = {
        // How does income correlate with divorce?
        
        // Checking for multiple conditions with
        // && throws an error for some reason
        val data: DataFrame = df
          .select("state_ab", "city", "zip_code", "family_median", "divorced")
          .filter("family_median != 'NaN' AND divorced != 'NaN'")

        // FIXME: city names are likely not unique b/w states
        val data_state: DataFrame = data
          .groupBy("state_ab")
          .agg(avg("family_median"), avg("divorced"))

        val data_city: DataFrame = data
          .groupBy("city")
          .agg(avg("family_median"), avg("divorced"))

        val data_place: DataFrame = data
          .groupBy("zip_code")
          .agg(avg("family_median"), avg("divorced"))


        data_state.show()
        data_city.show()
        data_place.show()
    }
}
