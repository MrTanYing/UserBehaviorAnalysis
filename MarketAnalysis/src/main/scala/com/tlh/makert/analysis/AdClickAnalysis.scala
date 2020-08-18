package com.tlh.makert.analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @Comment  页面广告点击量统计,达到标准加入黑名单继续统计点击量
 * @Author: tlh
 * @Date 2020/8/17 19:59
 * @Version 1.0
 */
//输入样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
//输出样例类
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
//侧输出流黑名单报警信息样例类
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //数据源
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)
    //基操
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 有恶意点击行为的用户(一天内点击同一广告100次)输出到侧输出流(黑名单报警)
      val filterBlackListUserStream = adLogStream
        .keyBy(data => (data.userId, data.adId))
        .process(new FilterBlackListUserResult(100))

      //开窗聚合统计
      val resultStream = filterBlackListUserStream
        .keyBy(_.province)
        .timeWindow(Time.hours(1), Time.seconds(5))
        .aggregate(new AdCountAgg(), new AdCountWindowResult())

    resultStream.print("count result")
    filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("waring")
    env.execute("Ad click analysis job")

  }

}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end,key,input.head))
  }
}

class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{
  lazy val countState : ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("count",classOf[Long]))
  lazy val resetTimerTsState : ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("reset-ts",classOf[Long]))
  lazy val isBlackState : ValueState[Boolean] = getRuntimeContext
    .getState(new ValueStateDescriptor[Boolean]("maxcount",classOf[Boolean]))
  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long),
    AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //获取当前计数值,待用
    val curCount = countState.value()
    //每一个用户第一个数据来了,需要直接注册零点的清空状态计时器
    if(curCount == 0){
      //timerService().currentProcessingTime() 获取当前处理时间
      val ts = (ctx.timerService().currentProcessingTime()/
        (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //判断count值是否达到定义的阀值,如果超过输出到黑名单
    if(curCount >= maxCount){
      //判断是否已经在黑名单里,没有则输入到侧输出流
      if(!isBlackState.value()){
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWarning]("warning"),BlackListUserWarning(value.userId,value.adId,"Click ad over " + maxCount + " times today."))
      }
      return
    }
    //正常情况,count加1,然后将数据原样输出
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]
    #OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if(timestamp == resetTimerTsState.value()){
      isBlackState.clear()
      countState.clear()
    }

  }
}
