import java.io.{File, PrintWriter, Writer}
import java.util

import com.wacai.stanlee.omega.bang.{SqlMaker, StringCombination}
import com.wacai.stanlee.omega.input.TableDescription
import com.wacai.stanlee.omega.output.{AggTableOutput, StatisticalTableOutput}
import com.wacai.stanlee.omega.util.BangJsonUtils
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}
import org.apache.spark.sql.hive.CliMain
import org.apache.spark.sql.types.StructType

/**
  * @author manshahua@wacai.com
  * @date 2017/12/14 下午7:00
  */
object TableTest {
  def main(args: Array[String]): Unit = {

//    val table:TableDescription = new TableDescription
//    val bangJsonUtils = BangJsonUtils.fromFile("tableDescription.json")
//    table.init(bangJsonUtils)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate
    val writer:Writer = new PrintWriter(new File("ddl_sql"))
    val resultData:Dataset[Row] = spark.read.table("iris_train_data")
    resultData.show()
    val columnNames:List[String] = resultData.columns.toList
    resultData.printSchema()
    val structType:StructType = resultData.schema
    structType.foreach(structField=>{
      writer.write(structField.name+" "+structField.dataType.typeName+",\n")
    })
    writer.close()
    println("ddddd")
//    val createdTrainformat2Table = spark.read.option("header", "true").csv("/Users/duty/Downloads/data_format2/train_format2_result1.csv")
//    createdTrainformat2Table.createOrReplaceTempView("train_format2")
//    spark.sql("drop table if exists ads_model_repeat_buyer_prediction_details2")
//    spark.sql("create table ads_model_repeat_buyer_prediction_details2\nas\nselect \n\tuser_id,\n\tage_range,\n\tgender,\n\tlabel,\n\tmerchant_id,\n\titem_id,\n\tcategory_id,\n\tbrand_id,\n\tfrom_unixtime(unix_timestamp(concat(\"2017\",time_stamp), 'yyyyMMdd')) as time_stamp,\n\taction_type\nfrom train_format2")
//    val createdTranformatTable = spark.read.option("header", "true").csv("/Users/duty/Downloads/data_format1/train_format1.csv")
//    createdTranformatTable.createOrReplaceTempView("train_format")
//    spark.sql("select * from ads_model_repeat_buyer_prediction_details_st limit 10").show()
//    StatisticalTableOutput.tableOutput(table, spark)
//    AggTableOutput.tableOutput(table, spark)
//    spark.sql("select * from ads_model_repeat_buyer_prediction_train limit 10").show()
    //中间新生成字段名的表名为原始表名+tmp，会保留原始表里所有的字段 生成的衍生变量结果表为原始表名+result，只有主key的字段和衍生的变量字段
//    spark.sql("select * from ads_model_repeat_buyer_prediction_details_tmp limit 10").show()
//    spark.sql("select * from ads_model_repeat_buyer_prediction_details_user_id_merchant_id_result limit 10").show()
//    spark.sql("create table ads_model_repeat_buyer_prediction_train\nas\nselect\n\tb.*,\n\ta.label\nfrom\n(\n\tselect \n\t\t*\n\tfrom train_format\n)a\njoin\n(\n\tselect\n\t\t*\n\tfrom ads_model_repeat_buyer_prediction_details_user_id_merchant_id_result\n)b\non (a.user_id = b.user_id and a.merchant_id = b.merchant_id)")
//    val dataReader = new DataFrameReader
//    val trainTable = dataReader.table("ads_model_repeat_buyer_prediction_train")
////    val columnNames = trainTable.columns
//    spark.sql("drop table if exists ads_model_repeat_buyer_prediction_details")
//    spark.sql("create table ads_model_repeat_buyer_prediction_details\nas\nselect\n\ta.user_id, a.merchant_id, a.label, a.tag, b.item_id,b.cat_id, b.brand_id, b.time_stamp, b.action_type\nfrom\n(\n\tselect\n\t\tuser_id,\n\t\tmerchant_id,\n\t\tlabel,\n\t\t\"train\" as tag\n\tfrom train_format\n\tunion all\n\tselect\n\t\tuser_id,\n\t\tmerchant_id,\n\t\t2 as label,\n\t\t\"test\" as tag\n\tfrom test_format\n)a\njoin\n(\n\tselect\n\t\tuser_id,\n\t\tseller_id,\n\t\titem_id,\n\t\tcat_id,\n\t\tbrand_id,\n\t\tfrom_unixtime(unix_timestamp(concat(\"2017\",time_stamp), 'yyyyMMdd')) as time_stamp,\n\t\taction_type\n\tfrom user_log_format\n)b\non (a.user_id = b.user_id and a.merchant_id = b.seller_id)\ngroup by a.user_id, a.merchant_id, a.label, a.tag, b.item_id,b.cat_id, b.brand_id, b.time_stamp, b.action_type")
//    spark.sql("create table user_log_format_train\nas\nselect\n\ta.*,\n\tb.item_id,\n\tb.cat_id,\n\tb.brand_id,\n\tb.time_stamp,\n\tb.action_type\nfrom\n(\n\tselect\n\t\t*\n\tfrom train_format\n)a\njoin\n(\n\tselect \n\t\t*\n\tfrom user_log_format\n)b\non (a.user_id = b.user_id and a.merchant_id = b.seller_id)")
//    println(columnNames.mkString("  "))
//    spark.sql("select ads_model_repeat_buyer_prediction_details_user_id_merchant_id_result")
//    spark.sql("create table ads_model_repeat_buyer_prediction_details2_result1\nas\nselect\n\ta.age_range,\n\ta.gender,\n\tb.*\nfrom\n(\n\tselect\t\n\t\tuser_id,\n\t\tage_range,\n\t\tgender\n\tfrom ads_model_repeat_buyer_prediction_details2\n\tgroup by user_id, age_range, gender\n)a\njoin\n(\n\tselect\n\t\t*\n\tfrom ads_model_repeat_buyer_prediction_details2_result\n)b\non (a.user_id = b.user_id)")
//    spark.sql("select * from ads_model_repeat_buyer_prediction_details2_result limit 10 ").show()
//    CliMain.startCliMain(args)

  }
}
