package com.tlh.analysis.hotitem

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/8/13 18:47
 * @Version 1.0
 */
object HotItems {

  def main(args: Array[String]): Unit = {

  }

}
//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long,categoryId: Int, behavior: String, timestamp: Long)

//窗口聚合结果样例类
case class ItemViewCount(itemId: Long,windowEnd: Long, count: Long)
