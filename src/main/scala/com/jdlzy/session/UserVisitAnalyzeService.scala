package com.jdlzy.session

import com.jdlzy.constants.Constants
import com.jdlzy.dao.factory.DAOFactory
import com.jdlzy.exception.TaskException
import com.jdlzy.javautils.ParamUtils
import com.jdlzy.scalaUtils.{InitUtils, SparkUtils}
import org.apache.spark.sql.DataFrame

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
    sc.stop()
  }
}
