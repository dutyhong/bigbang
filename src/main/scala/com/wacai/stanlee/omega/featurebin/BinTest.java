package com.wacai.stanlee.omega.featurebin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author manshahua@wacai.com
 * @date 2018/2/27 上午11:30
 */
public class BinTest {
    public static void main(String[] args)
    {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("bintest")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("/Users/duty/Documents/金融信贷算法资料/第二和三课代码/application.csv");
        dataset.createOrReplaceTempView("tmp");
        sparkSession.sql("select term, loan_status,count(member_id) from tmp group by term,loan_status").show();
    }
}
