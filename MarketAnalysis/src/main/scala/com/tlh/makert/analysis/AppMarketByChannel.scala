package com.tlh.makert.analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
/**
 * @Comment  APP市场推广统计 (渠道,行为)
 * @Author: tlh
 * @Date 2020/8/17 19:59
 * @Version 1.0
 */
object AppMarketByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.addSource(new SimulatedSource)
      .assignAscendingTimestamps(_.timestamp)

    //开窗统计
    val resultStream = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.days(1),Time.seconds(5))
      .process(new MarkCountbyChannel())

    resultStream.print()

    env.execute("app market by channel job")



  }

}

//输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
//输出数据样例类
case class MarketViewCount(windowStart: String, windowend: String, channel: String, behavior: String,count: Long)

// 自定义测试数据源
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior]{
  // 是否运行的标识位
  var running = true
  // 定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    // 定义一个生成数据最大的数量
    val maxCounts = Long.MaxValue
    var count = 0L

    // while循环，不停地随机产生数据
    while( running && count < maxCounts ){
      val id = UUID.randomUUID().toString
      //rand.nextInt(behaviorSet.size)
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }
}

//自定义 ProcessWindowFunction
class MarkCountbyChannel() extends ProcessWindowFunction[MarketUserBehavior,MarketViewCount, (String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior],
                       out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketViewCount(start,end,channel,behavior,count))

  }
}


