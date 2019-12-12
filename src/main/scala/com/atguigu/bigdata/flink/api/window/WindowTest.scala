package com.atguigu.bigdata.flink.api.window

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.streaming.api.scala._

object WindowTest {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		//val inputDataStream: DataStream[String] = env.readTextFile("input")
		val inputDataStream: DataStream[String] = env.socketTextStream("hadoop102", 9999)

		val dataStram: DataStream[SensorReading] = inputDataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			}
		)


		dataStram.print()
		env.execute("window")
	}
}
