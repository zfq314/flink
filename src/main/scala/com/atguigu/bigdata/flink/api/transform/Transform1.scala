package com.atguigu.bigdata.flink.api.transform

import org.apache.flink.streaming.api.scala._

object Transform1 {
	def main(args: Array[String]): Unit = {
		val env:     StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
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

		val waringData: DataStream[(String, Double)] = high.map(
			data => (data.id, data.temperature)
		)
		//coMap合并两个流，两个流的类型可以不一样
		val connectDataStream: ConnectedStreams[(String, Double), SensorReading] = waringData.connect(low)
		val coMap: DataStream[Product] = connectDataStream.map(
			waringData => (waringData._1, waringData._2, "高温"),
			lowData => (lowData, "常温")
		)
		//union合并多个流，多个流的类型必须一致
		val joinDataStream: JoinedStreams[SensorReading, SensorReading] = low.join(high)
		//low.union(coMap)
		coMap.print()

		env.execute()
	}
}
