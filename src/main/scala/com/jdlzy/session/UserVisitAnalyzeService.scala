package com.jdlzy.session

import java.text.SimpleDateFormat
import java.util.Date

import com.jdlzy.constants.Constants
import com.jdlzy.dao.factory.DAOFactory
import com.jdlzy.exception.TaskException
import com.jdlzy.javautils.{ParamUtils, StringUtils}
import com.jdlzy.scalaUtils.{AnalyzeHelperUtils, InitUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.json.JSONObject

/**
  * Created by liuziyang on 2017/9/11.
  * Copyright © liuziyang ustl. All Rights Reserved
  */


object UserVisitAnalyzeService {
  def main(args: Array[String]): Unit = {
val context=InitUtils.initSparkContext()
    val sc=context._1
    val sqlContext=context._2
    SparkUtils.loadLocalTestDataToTmpTable(sc,sqlContext)
    //创建dao组件，到组件是用来操作数据库的
    val taskDao = DAOFactory.getTaskDAO()
    // 通过任务常量名来获取任务ID,并将java.lang.Long转成scala.Long
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_SESSION_TASKID).longValue()
    val task = if (taskId > 0) taskDao.findById(taskId) else null
    // 抛出task异常
    if (task == null) {
      throw new TaskException("Can't find task by id: " + taskId);
    }
    // 获取任务参数
    val taskParam = new JSONObject(task.getTaskParam)
    println(taskParam)

    try {
      // 执行需求
      taskId match {
        /**
          * 需求2
          * 在指定日期范围内，按照session粒度进行数据聚合。要求聚合后的pair RDD的元素是<k:String,v:String>,
          * 其中k=sessionid  v的格式如下：sessionid=value|searchword=value|clickcaterory=value|age=value|
          * professional=value|city=value|sex=value（Spark RDD + Sql）
          */
        case 2L => {
          val sql = AnalyzeHelperUtils.getSQL(taskParam)
          val aggUserVisitAction = sqlContext.sql(sql._2).rdd
          val aggUserInfo = sqlContext.sql(sql._1).rdd
          val actionRddByDateRange = displaySession(aggUserInfo, aggUserVisitAction)
          print(actionRddByDateRange.collect().toBuffer)
        }

//        /**
//          * 需求3，根据用户的查询条件，一个 或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤，
//          * 注意：session时间范围是必选的。返回的结果RDD元素格式同上（Spark RDD + Sql）
//          */
//        case 3L => {
//          val actionRddByRequirement = sessionAggregateByRequirement(sQLContext, taskParam).collect().toBuffer
//          println(actionRddByRequirement)
//        }
//
//        /** 需求4
//          * 实现自定义累加器完成多个聚合统计业务的计算，统计业务包括访问时长：1~3秒，4~6秒，7~9秒，10~30秒，30~60秒的session访问量统计，
//          * 访问步长：1~3个页面，4~6个页面等步长的访问统计 注意：业务较为复杂,需要使用多个广播变量时，就会使得程序变得非常复杂，
//          * 不便于扩展维护（Spark Accumulator）
//          */
//        case 4L => {
//          val res = getVisitLengthAndStepLength(sc, sQLContext, taskParam)
//          println(res)
//        }
//
//        /** 需求5
//          * 对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类。
//          * 优先级：点击，下单，支付。二次排序（Spark）
//          */
//        case 5L => {
//          val session = getSessionByRequirement(sQLContext, taskParam)
//          val hotProducts = getHotCategory(session).iterator
//          for (i <- hotProducts)
//            print(i)
//        }
      }
    } catch {
      case e: Exception => println("没有匹配的需求编号")
    }

    sc.stop()
  }



  /**
    * 将输入的userInfo和userVisitAction按照指定形式展示出来,返回值形如(sessionid,
    * sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value)
    *
    * @param aggUserInfo
    * @param aggUserVisitAction
    * @return
    */
  def displaySession(aggUserInfo: RDD[Row], aggUserVisitAction: RDD[Row]): RDD[(String, String)] = {
    // sessionidRddWithAction 形为(session_id,RDD[Row])
    val sessionIdRddWithAction = aggUserVisitAction.map(tuple => (tuple.getString(2), tuple)).groupByKey()
    // userIdRddWithSearchWordsAndClickCategoryIds 形为(user_id,session_id|searchWords|clickCategoryIds)
    val userIdRddWithSearchWordsAndClickCategoryIds = sessionIdRddWithAction.map(f = s => {
      val session_id: String = s._1
      // 用户ID
      var user_id: Long = 0L
      // 搜索关键字的集合
      var searchWords: String = ""
      // 点击分类ID的集合
      var clickCategoryIds: String = ""
      //session的起始时间
      var startTime: Date = null
      // session的终止时间
      var endTime: Date = null
      // 访问步长
      var stepLength = 0

      val iterator = s._2.iterator
      while (iterator.hasNext) {
        val row = iterator.next()
        user_id = row.getLong(1)
        val searchWord = row.getString(6).trim
        val clickCategoryId = row.getString(7).trim
        if (searchWord != "null" && !searchWords.contains(searchWord)) {
          searchWords += (searchWord + ",")
        }
        if (clickCategoryId != "null" && !clickCategoryIds.contains(clickCategoryId)) {
          clickCategoryIds += (clickCategoryId + ",")
        }
        //  步长更新
        stepLength += 1

        val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val actionTime = TIME_FORMAT.parse(row.getString(4) + " " + row.getString(5))

        if (startTime == null && endTime == null) {
          startTime = actionTime
          endTime = actionTime
        } else if (actionTime.before(startTime)) {
          startTime = actionTime
        } else if (actionTime.after(endTime)) {
          endTime = actionTime
        }
      }
      // 访问时常
      val visitLength = (endTime.getTime - startTime.getTime) / 1000
      //val visitLength = 0
      searchWords = StringUtils.trimComma(searchWords)
      clickCategoryIds = StringUtils.trimComma(clickCategoryIds)
      val userAggregateInfo = Constants.FIELD_SESSION_ID + "=" + session_id + Constants.VALUE_SEPARATOR +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchWords + Constants.VALUE_SEPARATOR +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + Constants.VALUE_SEPARATOR +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + Constants.VALUE_SEPARATOR +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength
      (user_id, userAggregateInfo)
    })

    // userInfo形如(user_id,RDD[Row])
    val userInfo = aggUserInfo.map(tuple => (tuple.getLong(0), tuple))

    val userWithSessionInfoRdd = userInfo.join(userIdRddWithSearchWordsAndClickCategoryIds)

    userWithSessionInfoRdd.map(t => {
      val userAggregateInfo = t._2._2
      val userInfo = t._2._1
      val session_id = StringUtils.getFieldFromConcatString(userAggregateInfo, Constants.REGULAR_VALUE_SEPARATOR,
        Constants.FIELD_SESSION_ID)
      val age = userInfo.getInt(3)
      val professional = userInfo.getString(4)
      val city = userInfo.getString(5)
      val sex = userInfo.getString(6)

      // 形如(sessionid,sessionid=value|searchword=value|clickcategory=value|age=value|professional=value|city=value|sex=value)
      val aggregateInfo = userAggregateInfo + Constants.VALUE_SEPARATOR +
        Constants.FIELD_AGE + "=" + age + Constants.VALUE_SEPARATOR +
        Constants.FIELD_PROFESSIONAL + "=" + professional + Constants.VALUE_SEPARATOR +
        Constants.FIELD_CITY + "=" + city + Constants.VALUE_SEPARATOR +
        Constants.FIELD_SEX + "=" + sex
      (session_id, aggregateInfo)
    })
  }
}
