package com.wacai.stanlee.omega.featureselection;

import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午3:34
 */
public interface FeatureSelector {
     List<String> featureSelect(FeatureSelectorParam featureSelectorParam);

}
