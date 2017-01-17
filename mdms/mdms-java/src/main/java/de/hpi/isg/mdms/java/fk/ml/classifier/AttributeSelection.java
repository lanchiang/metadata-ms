package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 */
public class AttributeSelection {

    /**
     * Represent the information gain of each feature.
     */
    private Map<Feature, Double> infoGains;

    private double averageInfoGain = 0.0;

    /**
     * Find out the candidate attributes for the selection of the best attribute. The candidate attributes means
     * their information gain are above the average.
     */
    private final Set<Feature> selectCandidateAttribute(Dataset trainSet, Set<Feature> featureSet) {
        infoGains = new HashMap<>();
        for (Feature feature : featureSet) {
            double infoGain = calculateInfoGain(trainSet, feature);
            infoGains.putIfAbsent(feature, infoGain);
            averageInfoGain += infoGain;
        }
        averageInfoGain /= (double)featureSet.size();
        Set<Feature> candidateAttributes = new HashSet<>();
        for (Feature feature : infoGains.keySet()) {
            if (infoGains.get(feature)>averageInfoGain) {
                candidateAttributes.add(feature);
            }
        }
        return candidateAttributes;
    }

    public final Feature selectBestAttribute(Dataset trainingSet, Set<Feature> featureSet) {

        Set<Feature> candidateAttributes = selectCandidateAttribute(trainingSet, featureSet);

        double maxGainRatio = Double.NEGATIVE_INFINITY;
        Feature bestFeature = null;

        for (Feature feature : candidateAttributes) {
            double infoGain = infoGains.get(feature);
            double iv = calculateIntrinsicValue(trainingSet, feature);
            double gainRatio = infoGain/iv;
            if (gainRatio>maxGainRatio) {
                bestFeature = feature;
                maxGainRatio = gainRatio;
            }
        }
        return bestFeature;
    }

    private double calculateEntropy(Dataset trainSet) {
        Map<Instance.Result, Integer> instanceCountByClasses = trainSet.getInstanceCountByClasses();
        double entropy = 0.0;
        double dsSize = trainSet.getNumOfInstance();
        for (Instance.Result result : instanceCountByClasses.keySet()) {
            double pro = (double)instanceCountByClasses.get(result)/dsSize;
            entropy -= pro*Math.log(pro);
        }
        return entropy;
    }

    private double calculateEntropy(List<Instance> trainSet) {
        Map<Instance.Result, List<Instance>> instanceByClasses = trainSet.stream()
                .collect(Collectors.groupingBy(Instance::getLabel));
        double entropy = 0.0;
        double dsSize = trainSet.size();
        for (Instance.Result result : instanceByClasses.keySet()) {
            double pro = (double)instanceByClasses.get(result).size()/dsSize;
            entropy -= pro*Math.log(pro);
        }
        return entropy;
    }

    private double calculateInfoGain(Dataset trainSet, Feature feature) {
        double overallEntropy = calculateEntropy(trainSet);
        double partialEntorpy = 0.0;
        if (feature.getFeatureType() == Feature.FeatureType.Nominal) {
            Map<Object, List<Instance>> dsByFeatureValue = splitDatasetByFeatureValue(trainSet, feature);
            for (Map.Entry<Object, List<Instance>> entry : dsByFeatureValue.entrySet()) {
                partialEntorpy +=
                        (double)entry.getValue().size()/(double)trainSet.getDataset().size()*
                                calculateEntropy(entry.getValue());
            }
        }
        return overallEntropy - partialEntorpy;
    }

    private double calculateIntrinsicValue(Dataset trainSet, Feature feature) {
        double iv = 0.0;
        if (feature.getFeatureType() == Feature.FeatureType.Nominal) {
            Map<Object, List<Instance>> dsByFeatureValue = splitDatasetByFeatureValue(trainSet, feature);
            for (Map.Entry<Object, List<Instance>> entry : dsByFeatureValue.entrySet()) {
                double pro = (double)entry.getValue().size()/(double)trainSet.getDataset().size();
                iv -= pro*Math.log(pro);
            }
        }
        return iv;
    }

    public Map<Object, List<Instance>> splitDatasetByFeatureValue(Dataset trainingSet, Feature feature) {
        Map<Object, List<Instance>> dsByFeatureValue = new HashMap<>();
        trainingSet.getDataset().stream().forEach(instance -> {
            Object featureValue = instance.getValueByFeature(feature);
            dsByFeatureValue.putIfAbsent(featureValue, new ArrayList<>());
            dsByFeatureValue.get(featureValue).add(instance);
        });
        return dsByFeatureValue;
    }
}
