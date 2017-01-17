package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;

import java.util.Collection;
import java.util.Objects;

/**
 * Super class for various feature classes.
 * @author Lan Jiang
 */
abstract public class Feature implements FeatureUpdate {

    protected String featureName;

    /**
     * Indicate whether the feature is numeric or nominal.
     */
    protected FeatureType featureType;

    /**
     * Indicate the count of distinct value.
     */
    protected long distinctCount;

    /**
     * Indicate the unique value count in this dataset.
     */
    protected long uniqueCount;

    /**
     * Indicate the missing value count.
     */
    protected long missingCount;

    public String getFeatureName() {
        return featureName;
    }

    public enum FeatureType {
        /**
         * Indicates that the feature is numeric.
         */
        Numeric,
        /**
         * Indicates that the feature is nominal.
         */
        Nominal;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Feature feature = (Feature) o;
        return distinctCount == feature.distinctCount &&
                uniqueCount == feature.uniqueCount &&
                missingCount == feature.missingCount &&
                Objects.equals(featureName, feature.featureName) &&
                featureType == feature.featureType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, featureType, distinctCount, uniqueCount, missingCount);
    }
}
