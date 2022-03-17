package com.scalagenerator.model

//case is way to represent classes that are logical representation of data
case class ServerLog(
                    eventId: String,
                    accountId: Int,
                    eventType: String,
                    locationCountry: String,
                    eventTimeStamp: Long
                    ) extends Serializable {
  override def toString: String = f"$eventId%s,$accountId%s,$eventType%s,$locationCountry%s,$eventTimeStamp%s"
}


object ServerLog { //It is used to create new instances of ServerLog from a string representation
  def fromString(value: String): ServerLog = {
    val elements: Array[String] = value.split(",")
    ServerLog(elements(0), elements(1).toInt, elements(2), elements(3), elements(4).toLong)
  }
}