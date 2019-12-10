package com.atguigu.bigdata.flink.api.transform

import org.apache.flink.streaming.api.scala._

object File {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val inputDataStream: DataStream[String] = env.readTextFile("input")
		val wordCount: DataStream[(String, Int)] = inputDataStream.flatMap(_.split("\\s"))
			.map((_, 1))
			.keyBy(0)
			.sum(1)

		wordCount.print()

		env.execute()
	}
}
