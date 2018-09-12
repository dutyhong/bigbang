package com.wacai.stanlee.omega.featureselection;

import com.wacai.stanlee.omega.util.MapSort;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/22 上午10:38
 */
public class RandomForestSelector implements FeatureSelector {
    private Map<String, Double> totalFeaturesImportance = new HashMap<>();

    public Map<String, Double> getTotalFeaturesImportance() {
        return totalFeaturesImportance;
    }

    public void setTotalFeaturesImportance(Map<String, Double> totalFeaturesImportance) {
        this.totalFeaturesImportance = totalFeaturesImportance;
    }

    @Override
    public  List<String> featureSelect(FeatureSelectorParam featureSelectorParam)
    {
        Dataset<Row> samples = featureSelectorParam.getSamples();
        String[] featureNames = featureSelectorParam.getFeatureColumnNames();
        int selectedFeatureNum = featureSelectorParam.getSelectedFeatureNum();
        String labelName = featureSelectorParam.getLabelName();
        double threshold = featureSelectorParam.getSelectedFeatureThreshold();
        samples = samples.withColumn(labelName, functions.col(labelName).cast("long"));

        VectorAssembler va = new VectorAssembler().setInputCols(featureNames).setOutputCol("features");
        samples = va.transform(samples);
        Dataset<Row> transformedSamples = samples.select("features", labelName);
        transformedSamples.unpersist();
        //对特征进行标准变换，归一化等
        StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledfeatures").setWithStd(true).setWithMean(false);
        Dataset<Row> scaledFeatures = scaler.fit(transformedSamples).transform(transformedSamples);
        //随机深林分类器
        RandomForestClassifier rf = new RandomForestClassifier().setLabelCol(labelName).setFeaturesCol("scaledfeatures");//这个只是定义了一个分类器，并不是model，只有训练之后才是model
        RandomForestClassificationModel model = rf.fit(scaledFeatures);
        //取出所有特征的重要性值
//        Map<String, Double> featureImportanceMap = new HashMap<>();
        int length = featureNames.length;
        double[] importances = model.featureImportances().toArray();
        for(int i=0; i<length; i++)
        {
            double importance = importances[i];
            String feature = featureNames[i];
            totalFeaturesImportance.put(feature, importance);
        }
        //对所有特征根据重要性排序
        Map<String, Double> sortedFeatureImportantanceMap = MapSort.sortByValueDesc(totalFeaturesImportance);
//        int startIndex = length - selectedFeatureNum;
        int i = 0;
        List<String> selectedFeatures = new ArrayList<>();
        if(selectedFeatureNum>0) {
            for (Map.Entry<String, Double> entry : sortedFeatureImportantanceMap.entrySet()) {
                selectedFeatures.add(entry.getKey());
                i++;
                if (i >= selectedFeatureNum) {
                    break;
                }
            }
        }else{
            for(Map.Entry<String, Double> entry : sortedFeatureImportantanceMap.entrySet())
            {
                if(entry.getValue()<threshold)
                    break;
                selectedFeatures.add(entry.getKey());
            }
        }
        return selectedFeatures;
    }

    public static void main(String[] args)
    {

    }
}

