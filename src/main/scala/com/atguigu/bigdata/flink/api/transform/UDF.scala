package com.atguigu.bigdata.flink.api.transform

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.scala._

object UDF {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val inputDataStream: DataStream[String] = env.readTextFile("input")
		val filterDataStream: DataStream[String] = inputDataStream.filter(new FilterFilter("sensor_1"))
		//用匿名类来实现
		val filterNo: DataStream[String] = inputDataStream.filter(new RichFilterFunction[String] {
			override def filter(value: String): Boolean = {
				value.contains(" ")
			}
		})

		filterDataStream.print()
		println("----------")
		filterNo.print()
		env.execute()
	}
}

class FilterFilter(string: String) extends FilterFunction[String] {
	override def filter(value: String): Boolean = {
		value.contains(string)
	}
}