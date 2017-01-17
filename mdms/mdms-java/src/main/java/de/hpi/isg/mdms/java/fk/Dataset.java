package de.hpi.isg.mdms.java.fk;

import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represent a dataset holding a list of {@link Instance}
 * @author Lan Jiang
 */
public class Dataset {

    List<Instance> dataset;

    /**
     * Number of classes in this dataset.
     */
    private long numOfClasses;

    /**
     * Number of instances in this dataset.
     */
    private long numOfInstance;

    /**
     * The features used in this dataset.
     */
    List<Feature> features;

    public Dataset(List<Instance> dataset, List<Feature> features) {
        this.dataset = dataset;
        this.features = features;
    }

    public List<Instance> getDataset() {
        return dataset;
    }

    public long getNumOfClasses() {
        return numOfClasses;
    }

    public long getNumOfInstance() {
        return numOfInstance;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    /**
     * Calculate the statistics information of this dataset, i.e., number of classes and instances in this dataset.
     */
    public void buildDatasetStatistics() {
        numOfInstance = dataset.stream().count();

        Map<Instance.Result, List<Instance>> instanceByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getLabel));
        numOfClasses = instanceByClasses.entrySet().stream().count();
    }

    public void buildFeatureValueDistribution() {
        features.stream().forEach(feature -> feature.calcualteFeatureValue(dataset));
    }

    public Instance getInstanceByUnaryTuple(UnaryForeignKeyCandidate unaryForeignKeyCandidate) {
        Optional<Instance> result = this.dataset.stream()
                .filter(instance -> instance.getForeignKeyCandidate().equals(unaryForeignKeyCandidate))
                .findFirst();
        return result.orElse(null);
    }

    public void removeTestset(Dataset testset) {
        dataset.removeAll(testset.dataset);
        buildDatasetStatistics();
        buildFeatureValueDistribution();
    }

    public Map<Instance.Result, Integer> getInstanceCountByClasses() {
        Map<Instance.Result, Integer> instanceCountByClasses = new HashMap<>();
        Map<Instance.Result, List<Instance>> instanceByClasses = dataset.stream().collect(Collectors.groupingBy(Instance::getLabel));
        instanceByClasses.forEach((result, instances) -> instanceCountByClasses.put(result, instances.size()));
        return instanceCountByClasses;
    }

    /**
     * All the instances contain the same values on all the features.
     * @return
     */
    public boolean isSelfIndentity() {
        long distinctValueVector = dataset.stream().map(instance -> instance.getFeatureVector()).distinct().count();
        if (distinctValueVector==1) {
            return true;
        }
        else return false;
    }

    public Instance.Result findMajority() {
        Map<Instance.Result, Integer> instanceCountByClasses = getInstanceCountByClasses();
        Instance.Result maxResult = null;
        int maxCount = 0;
        for (Instance.Result result : instanceCountByClasses.keySet()) {
            if (instanceCountByClasses.get(result)>maxCount) {
                maxCount = instanceCountByClasses.get(result);
                maxResult = result;
            }
        }
        return maxResult;
    }
}
