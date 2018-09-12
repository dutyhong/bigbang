package com.wacai.stanlee.omega.featureselection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午7:17
 */
public class FeatureSelectorImp {
    private Map<String, Double> totalFeaturesImportance = new HashMap<>();
    //只对iv ks 这种分区间后的特征选择有用
    private Map<String, List<Double>> intervalValues  = new HashMap<>();
    private Map<String, IvIntervalStat> ivIntervalStat = new HashMap<>();
    //只对 相关系数有用
    private Map<String, Double> corrPositiveOrNegative = new HashMap<>();
    public List<String> featureSelect(FeatureSelectorParam featureSelectorParam)
    {
        String selectMethod = featureSelectorParam.getSelectMethod();
        List<String> selectedFeatureNames = new ArrayList<>();
        if("chi".equals(selectMethod))
        {
            ChiSquareSelector chiSquareSelector = new ChiSquareSelector();
            selectedFeatureNames = chiSquareSelector.featureSelect(featureSelectorParam);
        }else if("rf".equals(selectMethod))
        {
            RandomForestSelector randomForestSelector = new RandomForestSelector();
            selectedFeatureNames = randomForestSelector.featureSelect(featureSelectorParam);
            totalFeaturesImportance = randomForestSelector.getTotalFeaturesImportance();
        }else if("corr".equals(selectMethod))
        {
            PearsonCorrSelector pearsonCorrSelector = new PearsonCorrSelector();
            selectedFeatureNames = pearsonCorrSelector.featureSelect(featureSelectorParam);
            totalFeaturesImportance = pearsonCorrSelector.getTotalFeaturesImportance();
            corrPositiveOrNegative = pearsonCorrSelector.getCorrPositiveOrNegative();
        }else if("ks".equals(selectMethod))
        {
            KsSelector ksSelector = new KsSelector();
            selectedFeatureNames = ksSelector.featureSelect(featureSelectorParam);
            totalFeaturesImportance = ksSelector.getTotalFeaturesImportance();
        }else if("iv".equals(selectMethod))
        {
            IvSelector ivSelector = new IvSelector();
            selectedFeatureNames = ivSelector.featureSelect(featureSelectorParam);
            totalFeaturesImportance = ivSelector.getTotalFeaturesImportance();
            intervalValues = ivSelector.getIvIntervalValues();
            ivIntervalStat = ivSelector.getIvIntervalStats();
        }
        return selectedFeatureNames;
    }

    public Map<String, Double> getTotalFeaturesImportance() {
        return totalFeaturesImportance;
    }

    public void setTotalFeaturesImportance(Map<String, Double> totalFeaturesImportance) {
        this.totalFeaturesImportance = totalFeaturesImportance;
    }

    public Map<String, List<Double>> getIntervalValues() {
        return intervalValues;
    }

    public void setIntervalValues(Map<String, List<Double>> intervalValues) {
        this.intervalValues = intervalValues;
    }

    public Map<String, Double> getCorrPositiveOrNegative() {
        return corrPositiveOrNegative;
    }

    public void setCorrPositiveOrNegative(Map<String, Double> corrPositiveOrNegative) {
        this.corrPositiveOrNegative = corrPositiveOrNegative;
    }

    public Map<String, IvIntervalStat> getIvIntervalStat() {
        return ivIntervalStat;
    }

    public void setIvIntervalStat(Map<String, IvIntervalStat> ivIntervalStat) {
        this.ivIntervalStat = ivIntervalStat;
    }
}
