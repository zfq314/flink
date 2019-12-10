package com.atguigu.bigdata.flink.wc


import org.apache.flink.api.scala._

//批处理
object WordCount {
	def main(args: Array[String]): Unit = {
		//1、创建一个批处理的环境
		val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
		//2、从文件中读取数据
		val inputDataSet: DataSet[String] = environment.readTextFile("H:\\idea-workspace\\flink\\src\\main\\resources\\data.txt")
		//3、对数据进行处理
		val wordCount: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
			.map((_, 1))
			.groupBy(0)
			.sum(1)
			
		wordCount.print()
	}

}
