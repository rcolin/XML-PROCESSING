package org.thegreenseek.samples.kafka.fwk

import java.util.Properties
import java.io.{FileInputStream, IOException}

/**
  * Created by Macphil11 on 12/08/2016.
  */
object KafkaUtilities {
  def loadDefaultProperties: Properties = {
    val dprops :Properties = new Properties()
    try {
      dprops.loadFromXML(this.getClass.getResourceAsStream("DefaultKafkaProperties.xml"))
    } catch {
      case ioex: IOException => {
        println("COULD NOT LOAD DEFAULT KAFKA PROPS")
      }
    }
    return null
  }
}

class KafkaCore (props: Properties) {
  var cprops = props != null ? props : {
    var dprops = new Properties()
  }


}
