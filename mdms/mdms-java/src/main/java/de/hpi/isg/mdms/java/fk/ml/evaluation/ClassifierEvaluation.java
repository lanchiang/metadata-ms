package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Lan Jiang
 */
public class ClassifierEvaluation {

    protected Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth;

    protected Map<UnaryForeignKeyCandidate, Instance.Result> predicted;

    protected Instance.Result evaluatedLabel;

    private List<Metric> metrics = new ArrayList<>();

    private Map<String, Double> results = new HashMap<>();

    public ClassifierEvaluation(Instance.Result evaluatedLabel) {
        this.evaluatedLabel = evaluatedLabel;
    }

    public ClassifierEvaluation(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth,
                                Map<UnaryForeignKeyCandidate, Instance.Result> predicted,
                                Instance.Result evaluatedLabel) {
        this.groundTruth = groundTruth;
        this.predicted = predicted;
        this.evaluatedLabel = evaluatedLabel;
    }

    public void setGroundTruth(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth) {
        this.groundTruth = groundTruth;
    }

    public void setPredicted(Map<UnaryForeignKeyCandidate, Instance.Result> predicted) {
        this.predicted = predicted;
    }

    public void evaluate() {
        if (!checkIndentity()) return;
        metrics.stream().forEach(metric -> {
            metric.calculateMetric(groundTruth, predicted, evaluatedLabel);
            results.put(metric.getMetricName(), metric.getResult());
        });
    }

    public ClassifierEvaluation addMetric(Metric metric) {
        metrics.add(metric);
        return this;
    }

    private boolean checkIndentity() {
        if (groundTruth.size()!=predicted.size()) return false;

        return groundTruth.keySet().stream().allMatch(unaryForeignKeyCandidate -> predicted.keySet().contains(unaryForeignKeyCandidate));
    }

    public Map<String, Double> getResults() {
        return results;
    }

    public double getResultByMetricName(String name) {
        if (!results.containsKey(name)) {
            return Double.NaN;
        }
        return results.get(name);
    }

    //    public Object getEvaluation();
}
