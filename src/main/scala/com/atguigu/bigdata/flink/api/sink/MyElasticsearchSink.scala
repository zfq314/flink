package com.atguigu.bigdata.flink.api.sink

import java.util

import com.atguigu.bigdata.flink.api.transform.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object MyElasticsearchSink {
	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val inputDataStream: DataStream[String] = env.readTextFile("input")

		val dataStram = inputDataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				//匹配给样例类
				SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,
					dataArray(2).toDouble)
			})
		val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
		httpHosts.add(new HttpHost("hadoop102", 9200))
		val esBulider: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](httpHosts, new
				ElasticsearchSinkFunction[SensorReading] {
			override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
				println("save " + t + "successfully")
				val dataSource: util.HashMap[String, String] = new util
				.HashMap[String, String]()
				dataSource.put("id", t.id)
				dataSource.put("temperature", t.temperature.toString)
				dataSource.put("ts", t.timestamp.toString)

				val reueRequests = Requests.indexRequest().index("sensor").`type`("_doc").source(dataSource)
				requestIndexer.add(reueRequests)
			}
		})
		dataStram.addSink(esBulider.build())
		env.execute("es sink")
	}
}
