package com.jdlzy.scalaUtils

import com.jdlzy.constants.Constants
import com.jdlzy.javautils.{ParamUtils, SqlUnits}
import com.sun.rowset.internal.Row
import groovy.sql.Sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.json.JSONObject


/**
  * 辅助数据分析工具对象
  * Created by liuziyang on 2017/10/13.
  */
object AnalyzeHelperUtils {

  def getSQL(json:JSONObject):(String,String)={
    //解析json，获取用户的查询参数
    val startAge=ParamUtils.getSingleValue(json,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getSingleValue(json, Constants.PARAM_END_AGE)
    val startDate = ParamUtils.getSingleValue(json, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getSingleValue(json, Constants.PARAM_END_DATE)
    val professionals = ParamUtils.getMultipleValues(json, Constants.PARAM_PROFESSIONALS)
    val citys = ParamUtils.getMultipleValues(json, Constants.PARAM_CITYS)
    val sexs = ParamUtils.getMultipleValues(json, Constants.PARAM_SEX)
    val searchWords = ParamUtils.getMultipleValues(json, Constants.PARAM_SEARCH_WORDS)
    val categoryIds = ParamUtils.getMultipleValues(json, Constants.PARAM_CATEGORY_IDS)
    //准备sql查询userInfo表的语句
    var sqlUserInfo:String="SELECT * FROM "+Constants.TABLE_USER_INFO
    //如果有起始年林限定
    if(startAge!=null){
      val currentSql=" age >= "+startAge
      sqlUserInfo=SqlUnits.concatSQL(sqlUserInfo,currentSql)
    }
//如果有终止年龄限定
    if(endAge!=null){
      val currentSql=" age <= "+endAge
      sqlUserInfo=SqlUnits.concatSQL(sqlUserInfo,currentSql)
    }
    //如果有职业限定
    if(professionals!=null){
      val iterator=professionals.iterator
      var currentSql:String=""
      while(iterator.hasNext){
        val currentProfessional=iterator.next()
        currentSql+=(" profefssional = \""+ currentProfessional + "\" OR")
      }
      currentSql=SqlUnits.trimOr(currentSql)
      sqlUserInfo=SqlUnits.concatSQL(sqlUserInfo,currentSql)
    }
    // 如果有城市限定
    if (citys != null) {
      val iterator = citys.iterator
      var currentSql: String = ""
      while (iterator.hasNext) {
        val currentCity = iterator.next()
        currentSql += (" city = \"" + currentCity + "\" OR")
      }
      currentSql = SqlUnits.trimOr(currentSql)
      sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
    }
    //如果有性别限定
    if(sexs!=null){
      var currentSql:String=""
      for(sex<-sexs)currentSql+=(" sex = \""+sex + "\" OR")
      currentSql =SqlUnits.trimOr(currentSql)
      sqlUserInfo=SqlUnits.concatSQL(sqlUserInfo,currentSql)
    }
  // 准备sql查询user_visit_action表语句
    var sqlUserVisitAction:String="SELECT * FROM "+Constants.TABLE_USER_VISIT_ACTION
    //如果有日期限定
    if(startDate!=null){
      val currentSql=" date >= \""+startDate+"|""
        sqlUserVisitAction=SqlUnits.concatSQL(sqlUserVisitAction,currentSql)
    }
    //如果有关键字限定
    if(searchWords!=null){
      var currentSql:String=""
      for(searchWord<-searchWords) currentSql+=(" search_keyword ==\"" +searchWord +"\" OR")
    currentSql=SqlUnits.trimOr(currentSql)
      sqlUserVisitAction=SqlUnits.concatSQL(sqlUserVisitAction,currentSql)
    }
//如果有品类限定
    if(categoryIds!=null){
      var currentSql=""
      for(categoryId<-categoryIds) currentSql+=(" click_category_id = \"" +categoryId +"\"OR")
      currentSql=SqlUnits.trimOr(currentSql)
      sqlUserVisitAction=SqlUnits.concatSQL(sqlUserVisitAction,currentSql)
    }
    (sqlUserInfo,sqlUserVisitAction)
  }

  def getFullSession(sqlContext:SQLContext):RDD[Row]={
  val table=Constants.TABLE_USER_VISIT_ACTION
    val sql="SELECT * FROM "+table
    sqlContext.sql(sql).rdd
  }
}
