package queries

import clients.SparkClient

object Divorce {

    def main(args: Array[String]): Unit = {

        SparkClient.Connect()

        Query_1()

    }

    def Query_1(): Unit = {

        // How does the ratio between male and female correlate to divorce rates?
        
        //val columns: Array[String] = Array("male_pop", "female_pop", "divorced")
        val df = SparkClient.Data()
          .select("male_pop", "female_pop", "divorced")

        df.show()

    }

    def Query_2(): Unit = {
        // How does wealth correlate with divorce?
        // FIXME: what do with wealth
        // TODO: get divorce rates
    }
}
