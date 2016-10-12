package org.thegreenseek.samples.kafka.xml

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Macphil11 on 19/07/2016.
  */
class XMLKafkaProducer$Test extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("testParseXmlFile") {

    val buf = ArrayBuffer[String]()
    val topic: String = "landsat"

    def printMessage (prod: Any, cbuf: ArrayBuffer[String], ctopic:String): Int = {
        println("BUFFER PRINT START")
        println(cbuf.toString())
        println("BUFFER PRINT END")
        return 0
    }

    XMLKafkaProducer.parseXmlFile(
        "/Users/Macphil11/Documents/Projets/Hadoop/inputs_xml/landsat-small.xml",
        printMessage,
        null,
        buf,
        topic
      )

  }

}
