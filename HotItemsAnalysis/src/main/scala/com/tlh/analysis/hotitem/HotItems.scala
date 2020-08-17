package com.tlh.analysis.hotitem

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/13 18:47
 * @Version 1.0
 */
object HotItems {

  def main(args: Array[String]): Unit = {
    //0.环境初始化
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //定义事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.连接数据源读取数据
    val fileDataStream: DataStream[String]= env.readTextFile("D:\\IdeaProjects\\BigDataTlh\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //2.map转换
    val dataStream = fileDataStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)//创建时间戳与水印

    //3.计算逻辑
    //3.1 得到聚合窗口结果
    val aggStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5)) //大小一个小时,步长5分钟的滑动窗口
      .aggregate(new CountAgg(), new ItemWindowResult())

    //4.按窗口分组,统计当前矿口的商品count数并自定义处理流程
    val resultStream = aggStream
      .keyBy("windowEnd")
      .process(new TopHotItems(5))

    resultStream.print()

    env.execute("hot items")

  }

}
//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long,categoryId: Int, behavior: String, timestamp: Long)

//窗口聚合结果样例类
case class ItemViewCount(itemId: Long,windowEnd: Long, count: Long)

//自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L
  // 每来一条数据调用一次add，count值加一
  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数WindowFunction
class ItemWindowResult() extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId,windowEnd,count))
  }
}

//自定义keyProcessFunction
class TopHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

  //先定义状态: listState
  //ListState里可以是引用类型ItemViewCount,而ItemViewCount 里不能有引用类型

  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit =  {
     itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每一条数据来,直接记录到ListState
    itemViewCountListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定义一个listBuffer,保存liststate的数据
    val itemViewCounts :ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      itemViewCounts +=iter.next
    }

    //清空状态
    itemViewCountListState.clear()

    val sortedItemViewCounts = itemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val result = new StringBuffer

    result.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n")

    //遍历,格式化输入结果
    for (elem <- sortedItemViewCounts.indices) {
      val itemViewCount = sortedItemViewCounts(elem)
      result
        .append("NO").append(elem+1).append("\t")
        .append("热门ID = ").append(itemViewCount.itemId).append("\t")
        .append("热门度 = ").append(itemViewCount.count).append("\n")
    }
    result.append("==================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}

