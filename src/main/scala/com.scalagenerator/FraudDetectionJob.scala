package com.startdataengineering

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment //This is required to let Apache Flink know that this is a streaming data pipeline.

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "kafka:9092")

    //we specify the topic from which to read in data. In our case it is the server-logs topic
    val myConsumer = new FlinkKafkaConsumer[String](
      "server-logs", new SimpleStringSchema(), properties)
    myConsumer.setStartFromEarliest() //We configure our consumer to read from the very first event in that topic by using setStartFromEarliest

    //we read from addSource and create a variable representing a data stream of strings called events.
    val events = env
      .addSource(myConsumer)
      .name("incoming-events")

    //In this section we define how our incoming server log events are to be processed
    val alerts: DataStream[String] = events
      .keyBy(event => event.split(",")(1))
      .process(new FraudDetection)
      .name("fraud-detector")

    val myProducer = new FlinkKafkaProducer[String](
      "alerts", new SimpleStringSchema(), properties)

    alerts
      .addSink(myProducer)
      .name("send-alerts")

    //This data sink will enrich the event data, chunk the incoming events into batches of 10000 events and insert them into a PostgreSQL DB.
    events
      .addSink(new ServerLogSink)
      .name("event-log")

    //Execute the job
    env.execute("Fraud Detection")

  }

}