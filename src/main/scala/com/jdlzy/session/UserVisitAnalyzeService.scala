package com.jdlzy.session

import com.jdlzy.dao.factory.DAOFactory
import com.jdlzy.scalaUtils.{InitUtils, SparkUtils}

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
    val taskDao=DAOFactory.getTaskDAO

  }
}
