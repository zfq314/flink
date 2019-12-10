package com.atguigu.bigdata.flink.api.sink

import java.util.Properties

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * Kafka Sink
 */
object KafkaSink {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//val inputDataStream: DataStream[String] = env.readTextFile("input")
		//从kafka读取数据
		val properties: Properties = new Properties()

		properties.setProperty("bootstrap.servers", "hadoop102:9092")
		properties.setProperty("group.id", "consumer-group")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")

		val inputDataStream: DataStream[String] = env.addSource(new
				FlinkKafkaConsumer011[String]("sensor22", new
						SimpleStringSchema(), properties))

		val dataStram = inputDataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble).toString
			}
		)
		dataStram.addSink(new FlinkKafkaProducer011[String]
		("hadoop102:9092", "sensor23", new SimpleStringSchema()))
		env.execute("kafka sink")
	}
}
