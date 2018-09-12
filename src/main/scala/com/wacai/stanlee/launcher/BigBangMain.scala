package com.wacai.stanlee.launcher

import java.io.{File, PrintWriter}
import java.{lang, util}

import com.wacai.stanlee.omega.featureselection._
import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.output.FeatureGenerationImp
import com.wacai.stanlee.omega.sample.TableSample
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by passionke on 2017/11/28.
  * 但识码中趣 何劳键上音
  */
object BigBangMain {

  val log: Logger = LoggerFactory.getLogger(BigBangMain.getClass)
  //  var listCellCall = new util.ArrayList[CellCallDO]()
  //  FileUtils.deleteDirectory(new File("spark-warehouse"))
  //  FileUtils.deleteDirectory(new File("metastore_db"))
  //  log.info("BigBang")
  val filePath =  "file:///Users/duty/gitproj/bigbang/big-bang/spark-warehouse/"
//  val filePath = "hdfs://10.1.169.31:9000/data/program/big-bang/spark-warehouse/"
//  val filePath = "/data/program/big-bang/spark-warehouse/"
//  val filePath = "hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.d/"
//  val conf = new SparkConf()
//  val spark = {
//    SparkSession
//      .builder()
//      .config(conf)
//      .enableHiveSupport()
//      .getOrCreate()
//  }
  val spark: SparkSession = SparkSession.builder
      .appName("spark test")
      .master("local[*]")
      .enableHiveSupport
      .getOrCreate

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

//    val xfwhtcalls = spark.read.format("orc").load("hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.db/xf_wht_calls_6m_samples_small")//xf_wht_calls_6m_samples")
//        val xfwhtcalls = spark.read.format("parquet").option("header","true").load("/Users/duty/test.snapy.parquet")
    val xfwhtcalls = spark.read.format("csv").option("header", "true").load("/Users/duty/gitproj/bigbang/big-bang/src/main/resources/xf_wht_samples.csv")//("/Users/duty/Downloads/xf_idno_samples.csv")
//    xfwhtcalls.show(10)
//      xfwhtcalls.printSchema()
//    xfwhtcalls.createOrReplaceTempView("xf_wht_calls_6m_samples_final_result")
    val featureGenerationImp: FeatureGenerationImp = new FeatureGenerationImp
    val bangJsonUtils: BangJsonUtils = BangJsonUtils.fromFile("tableDescription.json")
    val tableDescription: TableDescription = new TableDescription
    tableDescription.init(bangJsonUtils)
    xfwhtcalls.createOrReplaceTempView("xf_wht_calls_6m_samples")
//    spark.sql("select count(distinct cd_use_time_2_months_6_other_cell_phone) from "+ "xf_wht_calls_6m_samples_final_result").show()
    //数据采样的过程
//    val tableSample:TableSample = new TableSample
//    tableSample.setSampleType("percent")
//    tableSample.setMemoryPercent(50)
//    tableSample.setTableName(tableDescription.getTableName)
//    val sampleTableName:String = tableSample.sample(spark)


//    tableDescription.setTableName(sampleTableName)
//    val start: Double = System.currentTimeMillis
    val tableNames: List[String] = featureGenerationImp.featureGenerationImp(spark, tableDescription)
    //List("xf_wht_calls_6m_samples_final_result")//
    val end: Double = System.currentTimeMillis
//    System.out.println("变量衍生所需时间为：" + (end - start) / (1000 * 60))
    val featureTable: FeatureTable = FeatureTable.fromFile("featureTable.json")

    val featureSelectorParam = new FeatureSelectorParam
    val featureSelector: FeatureSelectorImp = new FeatureSelectorImp
//        val tableNames = List("xf_wht_calls_6m_samples_final_result")//
    //    List("ads_model_repeat_buyer_prediction_details2_final_result1","ads_model_repeat_buyer_prediction_details2_final_result2",
    //    "ads_model_repeat_buyer_prediction_details2_final_result3","ads_model_repeat_buyer_prediction_details2_final_result4",
    //    "ads_model_repeat_buyer_prediction_details2_final_result5")
    //所有的特征和权值
    var featureImportances: mutable.Map[String, lang.Double] = mutable.Map()
    var ivIntervalStats: mutable.Map[String, IvIntervalStat] = mutable.Map()
    val writer1 = new PrintWriter(new File("rf_selected_features"))
    writer1.write("RF选择的特征：\n")
    val writer2 = new PrintWriter(new File("corr_selected_features"))
    writer2.write("corr选择的特征：\n")
    val writer3 = new PrintWriter(new File("iv_selected_features"))
    writer3.write("iv选择的特征：\n")
    var selectedFeatures = List[String]() //mutable.Buffer[String]()
    //对结果表进行逐表选择
    tableNames.foreach(tableName => {
      featureTable.setTableName(tableName)
      featureSelectorParam.init(featureTable, spark)
      println("特征个数为:"+featureSelectorParam.getFeatureColumnNames.size)
      //      测试RF选择器
      featureSelectorParam.setSelectMethod("rf")
      selectedFeatures = featureSelector.featureSelect(featureSelectorParam).asScala.toList
      var importance: mutable.Map[String, lang.Double] = featureSelector.getTotalFeaturesImportance.asScala
      for ((k, v) <- importance) {
        featureImportances(k) = v
        writer1.write(k + ":" + v + "\n")
      }
      writer1.flush()

      System.out.println("rf选择器  " + tableName + "  finished！！！！！")
      //相关系数选择器
//      featureSelectorParam.setSelectMethod("corr")
//      val corrStart = System.currentTimeMillis()
//      selectedFeatures = featureSelector.featureSelect(featureSelectorParam).asScala.toList
//      importance = featureSelector.getCorrPositiveOrNegative.asScala //featureSelector.getTotalFeaturesImportance.asScala
//      val corrEnd = System.currentTimeMillis()
//      println("相关系数选择器所需时间：" + (corrEnd - corrStart) / 1000)
//      for ((k, v) <- importance) {
//        featureImportances(k) = v
//        writer2.write(k + ":" + v + "\n")
//      }
//      writer2.flush()

      System.out.println("相关系数选择器 " + tableName + " finished！！！！！")
      //iv值
      featureSelectorParam.setSelectMethod("iv")
      val ivStart = System.currentTimeMillis()
      selectedFeatures = featureSelector.featureSelect(featureSelectorParam).asScala.toList
      System.out.println("iv选择器 " + tableName + " finished！！！！！")
      val ivEnd = System.currentTimeMillis()
      println("iv选择器所需时间为：" + (ivEnd - ivStart) / 1000)
      importance = featureSelector.getTotalFeaturesImportance.asScala
      for ((k, v) <- importance) {
        featureImportances(k) = v
        writer3.write(k + ":" + v + "\n")
      }
      //获得每个特征对应的iv的区间和各区间的好坏比例
      ivIntervalStats = featureSelector.getIvIntervalStat.asScala
      //取出所有特征计算iv时的分隔区间，将这些数据写道文件中
      writer3.write("每个特征分隔的区间如下：*********************\n\n")
      val ivIntervalValues: mutable.Map[String, util.List[lang.Double]] = featureSelector.getIntervalValues.asScala
      ivIntervalValues.foreach(ivIntervalValue => {
        val featureName: String = ivIntervalValue._1
        val intervalValues = ivIntervalValue._2
        writer3.write(featureName + ": ")
        val size: Int = intervalValues.size()
        0.until(size - 1).foreach(ind => {
          writer3.write(intervalValues.get(ind) + ",")
        })
        writer3.write(intervalValues.get(size - 1) + "\n")
      })
      writer3.flush()

      writer3.write("iv选择的特征区间信息如下：\n\n")
      ivIntervalStats.foreach(x => {
        val featureName = x._1
        val ivIntervalStat: IvIntervalStat = x._2
        val isMonotone = ivIntervalStat.isMonotone
        val intervalInfos: mutable.Map[String, String] = ivIntervalStat.getIntervalInfos.asScala
        intervalInfos.foreach(y => {
          writer3.write(featureName + "," + y._1 + "," + y._2 + ","+isMonotone+"\n")
        })
      })
      writer3.flush()
      //卡方校验
      //      featureSelectorParam.setSelectMethod("chi")
      //      selectedFeatures = featureSelector.featureSelect(featureSelectorParam).asScala
      //      writer.write("卡方校验选择的特征：\n"+selectedFeatures.mkString("\n"))
      //      writer.flush()
      //      System.out.println("卡方校验选择器finished！！！！！")

      //      System.out.println(selectedFeatures.mkString("\n"))
    })
    writer1.close()
    writer2.close()
    writer3.close()
    val lastEnd = System.currentTimeMillis()
//    println("整个工程所需时间为：" + (lastEnd - start) / 1000)
    //    CliMain.startCliMain(args)
  }


}
