package com.jdlzy.scalaUtils

import com.jdlzy.conf.ConfigurationManager
import com.jdlzy.constants.Constants
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by liuziyang on 2017/9/13.
  */
object SparkUtils {

  def loadLocalTestDataToTmpTable(sc:SparkContext,sqlContext:SQLContext):Unit={
    //读入用户session的日志数据并注册为临时表
    val SessionSchema=StructType(
      List(
      StructField("data",StringType,true),
        StructField("user_id",LongType,true),
        StructField("session_id", StringType, true),
        StructField("page_id", LongType, nullable = true),
        StructField("action_date",StringType,true),
        StructField("action_time", StringType, true),
        StructField("search_keyword", StringType, true),
        StructField("click_category_id", StringType, true),
        StructField("click_product_id", StringType, true),
        StructField("order_category_ids", StringType, true),
        StructField("order_product_ids", StringType, true),
        StructField("pay_category_ids", StringType, true),
        StructField("pay_product_ids", StringType, true),
        StructField("city_id", LongType, true)
      )
    )
//从指定位置创建RDD
    val session_path=ConfigurationManager.getProperty(Constants.LOCAL_SESSION_DATA_PATH)
    val sessionRDD=sc.textFile(session_path).map(_.split(" "))
    //将rdd映射成rowRDDD
    val sessionRowRDD=sessionRDD.map(s=>Row(
      s(0).trim,s(1).toLong,s(2).trim,
      s(3).toLong, s(4).trim, s(5).trim, s(6).trim, s(7).trim, s(8).trim,
      s(9).trim, s(10).trim, s(11).trim, s(12).trim,s(13).toLong
    ))
//将schema的信息应用到rowRDD上
    val sessionDataFrame=sqlContext.createDataFrame(sessionRowRDD,SessionSchema)
    //注册为临时sessionAction表
    sessionDataFrame.registerTempTable(Constants.TABLE_USER_VISIT_ACTION)
//2\定义用户数据schema
    val userSchema=StructType(
      List(
        StructField("user_id",LongType,nullable = true),
        StructField("username", StringType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("professional", StringType, true),
        StructField("city", StringType, true),
        StructField("sex", StringType, true)
      )
    )
    //从指定位置创建rdd
    val user_path=ConfigurationManager.getProperty(Constants.LOCAL_USER_DATA_PATH)
    val userRDD=sc.textFile(user_path).map(_.split(" "))
    val userRowRDD=userRDD.map(u=>{Row(u(0).toLong, u(2).trim, u(3).toInt, u(4).trim,
      u(5).trim, u(6).trim) })
    val userDataFrame=sqlContext.createDataFrame(userRowRDD,userSchema)
    //注册临时表
    userDataFrame.registerTempTable(Constants.TABLE_USER_INFO)
    //3\定义商品数据schema
    val productSchema=StructType(
      List(StructField("product_id",LongType,true),
        StructField("product_title",StringType,true),
        StructField("extend_info",StringType,true)))
      //从指定位置创建RDD
      val product_path=ConfigurationManager.getProperty(Constants.LOCAL_PRODUCT_DATA_PATH)
    val productRDD=sc.textFile(product_path).map(_.split(" "))
    val productRowRDD=productRDD.map(u => Row(u(0).toLong, u(1).trim, u(2).trim))
    val productDataFrame=sqlContext.createDataFrame(productRowRDD,productSchema)
    //注册为临时表
    productDataFrame.registerTempTable(Constants.TABLE_PRODUCT_INFO)

  }
}
