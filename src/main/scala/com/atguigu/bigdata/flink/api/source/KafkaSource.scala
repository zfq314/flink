package com.atguigu.bigdata.flink.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaSource {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment
			.getExecutionEnvironment

		//从kafka读取数据
		val properties: Properties = new Properties()

		properties.setProperty("bootstrap.servers", "hadoop102:9092")
		properties.setProperty("group.id", "consumer-group")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")

		val stream: DataStream[String] = env.addSource(new
				FlinkKafkaConsumer011[String]("sensor22", new
						SimpleStringSchema(), properties))
		stream.print()
		env.execute()
	}

}
