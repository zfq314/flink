package com.atguigu.bigdata.flink.api.transform

import org.apache.flink.streaming.api.scala._

object Transform {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//分组聚合
		val inputDataStream: DataStream[String] = env.readTextFile("input")
		val dataStram: DataStream[SensorReading] = inputDataStream.map(
			data => {
				data
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).trim.toDouble)
			}
		)
		//	.keyBy("id")
		//	.reduce((x, y) => SensorReading(x.id, y.timestamp + 10, x.temperature + 1))
		//	.sum("temperature")

		val splitDataStream: SplitStream[SensorReading] = dataStram.split(
			data =>
				if (data.temperature > 30) {
					Seq("high")
				} else {
					Seq("low")
				}
		)
		val high: DataStream[SensorReading] = splitDataStream.select("high")
		val low: DataStream[SensorReading] = splitDataStream.select("low")
		val all: DataStream[SensorReading] = splitDataStream.select("low", "high")
		high.print("high")
		low.print("low")
		all.print("all")
		env.execute()
	}
}
