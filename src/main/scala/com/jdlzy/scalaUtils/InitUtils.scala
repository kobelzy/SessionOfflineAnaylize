package com.jdlzy.scalaUtils

import com.jdlzy.conf.ConfigurationManager
import com.jdlzy.constants.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 初始化spark环境工具对象
  * Created by liuziyang on 2017/9/11.
  * Copyright © liuziyang ustl. All Rights Reserved
  */
object InitUtils {
  /**
    * 初始化spaark，sql环境
    * @return
    */
  def initSparkContext(): (SparkContext, SQLContext) = {
    val conf = getSparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = getSqlContext(sc)

    Logger.getRootLogger.setLevel(Level.OFF)
    (sc, sqlContext)

  }

  /**
    * 加载sql上下文环境，如果是在本地运行那么生成sql环境
    *
    * @param sc
    * @return
    */
  def getSqlContext(sc: SparkContext): SQLContext = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      new SQLContext(sc)
    } else {
      new HiveContext(sc)
    }
  }

  /**
    * 记载spark配置，如果本地，使用local，集群那么提交作业时候指定
    * @return
    */
  def getSparkConf(): SparkConf = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
        .setMaster(Constants.SPARK_MASTER)
    } else {
      new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
    }

  }
}
