package clients

import models.Person

/** This object handles connecting to Spark
 *
 */
object SparkClient extends Client[Person] {

    override def Connect(): Unit = {

    }

    override def Create(t: Person): Unit = {

    }

    override def Read(): Person = {

        return null
    }

    override def Update(t: Person): Unit = {

    }

    override def Delete(t: Person): Unit = {

    }

}
