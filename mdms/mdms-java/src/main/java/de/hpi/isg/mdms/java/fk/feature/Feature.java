package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;

import java.util.Collection;

/**
 * Super class for various feature classes.
 * @author Lan Jiang
 */
abstract public class Feature implements FeatureUpdate{

    protected String featureName;

    /**
     * Indicate whether the feature is numeric or nominal.
     */
    protected FeatureType featureType;

    public enum FeatureType {
        Nominal,

        Numeric,

        String;
    }

    public String getFeatureName() {
        return featureName;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public void setFeatureType(FeatureType featureType) {
        this.featureType = featureType;
    }
}
