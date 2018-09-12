package com.wacai.stanlee.omega.featurebin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2018/3/6 上午10:33
 */
public class KsMerge {
    public List<Double> splitBin(Dataset<Row> singleFeatureSamples, List<Double> originalBins, double overAllRate,
                          int maxInterval, String featureName, String labelName)
    {
        //

        return null;
    }
    public static void binaryFind(int[] arrs, int start, int end)
    {
        if(start==end)
        {
            return ;
        }
        int max= -2;
        int maxIndex = start;
        for(int i=start; i<end; i++)
        {
            if(arrs[i]>max)
            {
                max = arrs[i];
                maxIndex = i;
            }
        }
        System.out.println("最大值为：" + max+"最大值索引为："+maxIndex);
        binaryFind(arrs, start, maxIndex);
        binaryFind(arrs, maxIndex+1, end);
    }
    //二分查找
    public static int binaryFind(int[] arrs, int start, int end, int value)
    {
        int middle = (start+end)/2;
        if(start==end&&value!=arrs[middle])
        {
            System.out.println("没找到");
            return -1;
        }
        if(start==end&&value==arrs[middle])
        {
            System.out.println("找到 第 "+middle);
            return middle;
        }
        if(value>arrs[middle])
        {
            return binaryFind(arrs, middle+1, end, value);
        }else
        {
            return binaryFind(arrs, start, middle, value);
        }
    }
    public static void main(String[] args)
    {
        int[] arrs = {2,3,1,4,6,8,5,10,9,0};
        Arrays.sort(arrs);
        int a = binaryFind(arrs, 0, 10, 8);
        System.out.println("查找的值索引为："+a);
    }
}
