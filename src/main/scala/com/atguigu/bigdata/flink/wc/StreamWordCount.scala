package com.atguigu.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

//流处理
object StreamWordCount {
	def main(args: Array[String]): Unit = {
		val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val param: ParameterTool = ParameterTool.fromArgs(args)
		val host: String = param.get("host")
		val port: Int = param.getInt("port")
		val inputDataStream: DataStream[String] = executionEnvironment.socketTextStream(host,port)
		val wordCountDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split("\\s"))
			.map((_, 1))
			.keyBy(_._1)
			.sum(1)
		wordCountDataStream.print().setParallelism(1)
		//
		executionEnvironment.execute("stream word count")
	}
}
