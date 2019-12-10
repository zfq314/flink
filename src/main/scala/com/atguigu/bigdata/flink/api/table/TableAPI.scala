package com.atguigu.bigdata.flink.api.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

object TableAPI {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

	}
}
