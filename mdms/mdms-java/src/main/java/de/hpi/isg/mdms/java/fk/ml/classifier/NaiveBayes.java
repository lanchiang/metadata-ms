package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A concrete Naive Bayes classifier.
 * @author Lan Jiang
 */
public class NaiveBayes extends AbstractClassifier {

    /**
     * Indicate the prior probability, i.e. p(c)
     */
    private Map<Instance.Result, Double> priorProbability;

    /**
     * Indicate the likelyhoods, i.e. p(x|c). The keys and values in the map
     * stand for <FeatureName, <Label, <Value, Count>>>.
     */
    private Map<String, Map<Instance.Result, Map<Object, Double>>> likelyhoods;
//    private List<PartialLikelyhoods> likelyhoods;

    private Map<Instance.Result, List<Instance>> instancesByClasses;

    public NaiveBayes() {
        priorProbability = new HashMap<>();
        likelyhoods = new HashMap<>();
//        likelyhoods = new ArrayList<>();
    }

    private void calcultePriorProbability() {
        instancesByClasses.entrySet().stream().forEach(entry -> {
            double prior = (entry.getValue().size() + 1.0) / (trainingset.getDataset().size() + 1.0 * trainingset.getNumOfClasses());
            priorProbability.put(entry.getKey(), prior);
        });
    }

    private void calculateLikelyhoods() {
        List<Feature> features = trainingset.getFeatures();
        features.stream().forEach(feature -> {
            String featureName = feature.getFeatureName();
            instancesByClasses.entrySet().stream().forEach(entry -> {
                Map<Object, Double> partialfeatureValue = new HashMap<>();
                entry.getValue().stream().map(Instance::getFeatureVector).flatMap(map -> map.entrySet().stream())
                        .filter(stringObjectEntry -> stringObjectEntry.getKey().equals(featureName))
                        .forEach(stringObjectEntry -> {
                            Object value = stringObjectEntry.getValue();
                            if (partialfeatureValue.containsKey(value)) {
                                partialfeatureValue.put(value, partialfeatureValue.get(value)+1.0);
                            } else {
                                partialfeatureValue.put(value, 1.0);
                            }
                        });
                partialfeatureValue.entrySet().stream().forEach(pfventry -> {
                    partialfeatureValue.put(pfventry.getKey(), (pfventry.getValue() + 1.0) / (entry.getValue().size() + partialfeatureValue.size()));
                });
                if (likelyhoods.containsKey(featureName)) {
                    likelyhoods.get(featureName).put(entry.getKey(), partialfeatureValue);
                } else {
                    Map<Instance.Result, Map<Object, Double>> featureValueByClass = new HashMap<>();
                    featureValueByClass.put(entry.getKey(), partialfeatureValue);
                    likelyhoods.put(featureName, featureValueByClass);
                }
//                PartialLikelyhoods partialLikelyhoods = new PartialLikelyhoods(featureName, entry.getKey());
//                partialLikelyhoods.setValueDistribution(partialfeatureValue);
//                likelyhoods.add(partialLikelyhoods);
            });
        });

//        instancesByClasses.entrySet().forEach(resultListEntry -> {
//            features.stream().forEach(feature -> {
//                PartialLikelyhoods partialLikelyhoods =
//                        new PartialLikelyhoods(feature.getFeatureName(), resultListEntry.getKey());
//                List<Object> values = resultListEntry.getValue().stream()
//                        .map(Instance::getFeatureVector).flatMap(map -> map.entrySet().stream())
//                        .filter(entry -> entry.getKey().equals(feature.getFeatureName()))
//                        .map(Map.Entry::getValue).collect(Collectors.toList());
//                partialLikelyhoods.calculateValueDistribution(values);
//                partialLikelyhoods.getValueDistribution().entrySet().stream()
//                        .forEach(objectDoubleEntry ->
//                                partialLikelyhoods.getValueDistribution().put(objectDoubleEntry.getKey(),objectDoubleEntry.getValue()+1.0));
//            });
//        });
    }

    @Override
    public void train() {
        instancesByClasses = trainingset.getDataset().stream()
                .collect(Collectors.groupingBy(Instance::getLabel));
        calcultePriorProbability();
        calculateLikelyhoods();
    }

    @Override
    public void predict() {
        testset.getDataset().forEach(instance -> {
            double max = Double.NEGATIVE_INFINITY;
            Instance.Result maxResult = Instance.Result.UNKNOWN;
            for (Instance.Result label : Instance.Result.values()) {
                if (label.equals(Instance.Result.UNKNOWN)) continue;
                double result = 0.0;
                for (String feature : instance.getFeatureVector().keySet()) {
                    if (likelyhoods.get(feature).get(label)
                            .containsKey(instance.getFeatureVector().get(feature))) {
                        result += Math.log(likelyhoods.get(feature).get(label).get(instance.getFeatureVector().get(feature)));
                    } else {
                        result += Math.log(1.0 / (instancesByClasses.get(label).size() + likelyhoods.get(feature).get(label).size()));
                    }
                }
                result += Math.log(priorProbability.get(label));
                if (result > max) {
                    max = result;
                    maxResult = label;
                }
            }
            instance.setLabel(maxResult);
        });
    }

    @Override
    public void buildClassifier(Dataset dataset) {

    }

    @Override
    public void classifyInstance(Instance instance) {

    }

    public Map<Instance.Result, Double> getPriorProbability() {
        return priorProbability;
    }

    public Map<String, Map<Instance.Result, Map<Object, Double>>> getLikelyhoods() {
        return likelyhoods;
    }

//    public class PartialLikelyhoods {
//        private String featureName;
//        private Instance.Result label;
//        private Map<Object, Double> valueDistribution;
//
//        public PartialLikelyhoods(String featureName, Instance.Result label) {
//            this.featureName = featureName;
//            this.label = label;
//            valueDistribution = new HashMap<>();
//        }
//
////        public void calculateValueDistribution(List<Object> values) {
////            Map<Object, Long> result = values.stream().
////                    collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
////            for (Map.Entry<Object, Long> entry : result.entrySet()) {
////                valueDistribution.put(entry.getKey(), (double)entry.getValue());
////            }
////        }
//
//
//        public String getFeatureName() {
//            return featureName;
//        }
//
//        public Map<Object, Double> getValueDistribution() {
//            return valueDistribution;
//        }
//
//        public void setValueDistribution(Map<Object, Double> valueDistribution) {
//            this.valueDistribution = valueDistribution;
//        }
//    }
}
