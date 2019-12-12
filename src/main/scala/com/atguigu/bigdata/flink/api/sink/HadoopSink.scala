package com.atguigu.bigdata.flink.api.sink

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink


//写文件写入都hadoop中
object HadoopSink {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		val dataStream: DataStream[String] = env.readTextFile("input")
		val mapDataStream: DataStream[String] = dataStream.map(
			data => {
				val dataArrays: Array[String] = data.split(",")
				SensorReading(dataArrays(0).trim, dataArrays(1).trim.toLong,
					dataArrays(2).trim.toDouble).toString
			}
		)
		mapDataStream.addSink(new BucketingSink[String]("hdfs://hadoop102:9000/flink"))

		env.execute("hadoop sink")
	}
}
