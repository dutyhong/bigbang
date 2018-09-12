package com.wacai.stanlee.omega.input

import com.wacai.stanlee.omega.dataFormat._
import com.wacai.stanlee.omega.util.BangJsonUtils

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConverters._


/**
  * @author manshahua@wacai.com
  * @date 2017/12/13 下午4:47
  */
class TableDescription {
  //表注释
  @BeanProperty
  var desc: String = _
  //表名
  @BeanProperty
  var tableName: String = _
  //标签列，不包含则只做衍生不做特征选择
  @BeanProperty
  var labelName: String = _
  //分区数
  @BeanProperty
  var partitionNum:Int = _
  //表的字段解释
  @BeanProperty
  var fieldInfoList: List[FieldInfoJava] = List() //= new ArrayList[FieldInfo]()
  //对于数值型的字段切分需求
  @BeanProperty
  var numericSliceDOList: List[NumericSliceDOJava]=List() // = new ArrayList[NumericSliceDO]()
  //对于categorical类型的字段切分需求
  @BeanProperty
  var categoricalSliceDOList: List[CategoricalSliceDOJava]=List()//= new ArrayList[CategoricalSliceDO]()
  //对于日期类型的字段切分需求
  @BeanProperty
  var dateSliceDOList: List[DateSliceDOJava]=List() //= new ArrayList[DateSliceDO]()
  //对于用户自定义的字段切分需求
  @BeanProperty
  var userDefinedSliceDOList: List[UserDefinedSliceDOJava] = List()
  //通过要求切分的字段生成的新的字段
  @BeanProperty
  var newFields: Map[String, List[String]]=Map()
  //生成数值型的新字段run的sql
  @BeanProperty
  var numericSqlString: List[String] = List() //= new ArrayList[String]()
  //同上
  @BeanProperty
  var categoricalSqlString: List[String]= List()//= new ArrayList[String]()
  //同上
  @BeanProperty
  var dateSqlString: List[String]= List() // = new ArrayList[String]()
  //同上
  @BeanProperty
  var userDefinedSqlString: List[String]= List() // = new ArrayList[String]()
  //可以作为主键的字段
  @BeanProperty
  var keyFields: List[String] = List()
  //对某些字段做某些聚合函数
  @BeanProperty
  var fieldAggMethods: Map[String, List[String]] = Map()
  //总的切分个数
  @BeanProperty
  var totalSliceNum = 0
  //总的聚合方法数
  @BeanProperty
  var totalAggMethodsNum = 0
  def init(bangJsonUtils:BangJsonUtils): Unit = {
    desc = bangJsonUtils.getTableDesc //"天池数据表"
    tableName = bangJsonUtils.getTableName //"ads_model_repeat_buyer_prediction_details"
    labelName = bangJsonUtils.getLabelName
    partitionNum = bangJsonUtils.getPartitionNum
//    fieldInfoList = bangJsonUtils.getFieldInfos.asScala.toList //List(fieldInfo1, fieldInfo2, fieldInfo3, fieldInfo4, fieldInfo5, fieldInfo6, fieldInfo7,
//      fieldInfo8, fieldInfo9)
    //取出所有的group by key
    keyFields = bangJsonUtils.getPrimaryKeys.asScala.toList
    //初始化是numeric slice do
    val numericSliceDOs = bangJsonUtils.getNumericSliceDOs
    //处理空值
    if(null==numericSliceDOs) {
      numericSqlString = Nil
    }
    else{
        numericSliceDOList = numericSliceDOs.asScala.toList //numericSliceDOList :+ numericSliceDO
        //通过输入数据生成sql并将生成的新字段名放入map
        numericSliceDOList.foreach(numericSliceDO1 => {
          val fieldName: String = numericSliceDO1.getFieldName
          val min: Double = numericSliceDO1.getMin
          val max: Double = numericSliceDO1.getMax
          val sliceNum: Int = numericSliceDO1.getSliceNum
          //通过 fieldName 新生成的字段名字
          var numericNewFieldNames: List[String] = List() //= new ArrayList[String]()
          for (j <- 0 until sliceNum) {
            val sqlTmp: String = "case when " + fieldName +" >= "+((max - min) / sliceNum * (j)).formatted("%.2f")+
              " and "+fieldName + " < " + ((max - min) / sliceNum * (j + 1)).formatted("%.2f") + " then true else false end as " + fieldName + "_" + (j + 1)
            numericSqlString = numericSqlString :+ sqlTmp
            numericNewFieldNames = numericNewFieldNames :+ (fieldName + "_" + (j + 1))
          }
          newFields += (fieldName -> numericNewFieldNames)
        })
      }

    //初始化categorical slice do
    val categoricalSliceDOs = bangJsonUtils.getCategoricalSliceDOs
//    categoricalSliceDOs match {
    if(null==categoricalSliceDOs) {
      categoricalSqlString = Nil
    }
    else{
      categoricalSliceDOList = categoricalSliceDOs.asScala.toList
      //categoricalSliceDOList :+ categoricalSliceDO
      //categorical
      categoricalSliceDOList.foreach(categoricalSliceDO1 => {
        val categoricalValues1: List[String] = categoricalSliceDO1.getCategoricalValues.asScala.toList
        val fieldName: String = categoricalSliceDO1.getFieldName
        //通过field 生成新的字段名
        var categoricalNewFieldNames: List[String] = List()
        categoricalNewFieldNames = categoricalValues1.map(value => {
          val sqlString: String = "case when " + fieldName + "=" + "\'" + value + "\'" + " then true else false end as " + fieldName + "_" + value
          categoricalSqlString = categoricalSqlString :+ sqlString
          fieldName + "_" + value
        })
        newFields += (fieldName -> categoricalNewFieldNames)
      })
    }
    //初始化date slice do
    val dateSliceDOs = bangJsonUtils.getDateSliceDOs
    if(null==dateSliceDOs) {
      dateSqlString = Nil
    }
    else{
        dateSliceDOList = dateSliceDOs.asScala.toList//dateSliceDOList:+dateSliceDO
        //date
        dateSliceDOList.foreach(dateSliceDO1=>{
          val dateSliceType = dateSliceDO1.getSliceType
          val dateSliceTypeInterval = dateSliceDO1.getSliceTypeInterval
          val sliceNum = dateSliceDO1.getSliceNum
          val endTime = dateSliceDO1.getEndTime
          val fieldName = dateSliceDO1.getFieldName
          var dateNewFiledNames:List[String] = List()
          for(j<- 0 until sliceNum)
          {
            val function = dateSliceType match {
              case "year" => "udfyear"
              case "month" => "months_between"
              case "day" =>"udfday"
              case _ => throw  new RuntimeException("此切分暂不支持")
            }
            val tmpSql = "case when " + function + "(\'" + endTime  + "\', " + fieldName +") < " + (j+1)*dateSliceTypeInterval +
              " then true else false end as " + fieldName + "_" + (j+1)
            dateSqlString = dateSqlString:+tmpSql
            dateNewFiledNames = dateNewFiledNames:+ (fieldName + "_" + (j+1))
          }
          newFields+= (fieldName -> dateNewFiledNames)
        })
    }
    //初始化userdefined slice do
    val userDefinedSliceDOs = bangJsonUtils.getUserDefinedSliceDOs
    if(null==userDefinedSliceDOs) {
      userDefinedSqlString = Nil
    }
    else
    {
        userDefinedSliceDOList = userDefinedSliceDOs.asScala.toList //userDefinedSliceDOList:+userDefinedSliceDO
        //userdefined
        userDefinedSliceDOList.foreach(userDefinedSliceDO=>{
          //      var tmpSql:String
          val min = userDefinedSliceDO.getMin
          val max = userDefinedSliceDO.getMax
          val expression = userDefinedSliceDO.getExpression
          val fieldName = userDefinedSliceDO.getFieldName
          val sliceNum = userDefinedSliceDO.getSliceNum
          var userDefinedNewFields = List[String]()
          for(j<- 0 until(sliceNum))
          {
            val tmpSql = "case when ("+expression+") <= " + ((max - min) / sliceNum*(j+1)).formatted("%.2f")+ " then true else false end as " + fieldName + "_" + (j+1)
            userDefinedSqlString = userDefinedSqlString:+tmpSql
            userDefinedNewFields = userDefinedNewFields:+(fieldName+"_"+(j+1))

          }
          newFields+= (fieldName->userDefinedNewFields)
        })
    }



//*****************************一个问题没有解决：Map<String, List<String>> 里面的list谁util list转换为scala的时候是scala的list不能转
    val fieldAggMethodsJava = bangJsonUtils.getFieldAggMethods.asScala.toList
    fieldAggMethodsJava.foreach(f=>{
      val key = f._1
      val value = f._2.asScala.toList
      fieldAggMethods += (key->value)
    })

//    放入对不同字段采取不同的聚合函数
//    val aggMethods1 = List("c", "cd")
//    fieldAggMethods+= ("item_id"->aggMethods1)
//    val aggMethod2 = List("c")
//    fieldAggMethods += ("brand_id"->aggMethod2)

  }
}


