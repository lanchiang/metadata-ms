package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 */
public class FMeasure extends Metric {

    private double beta = 1.0;

    private double fscore;

    public FMeasure() {
    }

    public FMeasure(double beta) {
        this.beta = beta;
    }

    @Override
    public void calculateMetric(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth,
                                Map<UnaryForeignKeyCandidate, Instance.Result> predicted,
                                Instance.Result evaluatedLabel) {
        double precision = precision(groundTruth, predicted, evaluatedLabel);
        double recall = recall(groundTruth, predicted, evaluatedLabel);
        fscore = (1+Math.pow(beta, 2.0))*precision*recall/(Math.pow(beta, 2.0)*precision+recall);
    }

    private double precision(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth,
                             Map<UnaryForeignKeyCandidate, Instance.Result> predicted,
                             Instance.Result evaluatedLabel) {
        List<UnaryForeignKeyCandidate> predictedPositive = predicted.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .map(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getKey())
                .collect(Collectors.toList());
        long truePositive = groundTruth.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> predictedPositive.contains(unaryForeignKeyCandidateResultEntry.getKey()))
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .count();
        return (double)truePositive / (double)predictedPositive.size();
    }

    private double recall(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth,
                          Map<UnaryForeignKeyCandidate, Instance.Result> predicted,
                          Instance.Result evaluatedLabel) {
        List<UnaryForeignKeyCandidate> actualPositive = groundTruth.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .map(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getKey())
                .collect(Collectors.toList());
        long truePositive = predicted.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> actualPositive.contains(unaryForeignKeyCandidateResultEntry.getKey()))
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .count();
        return (double)truePositive / (double)actualPositive.size();
    }

    public double getFscore() {
        return fscore;
    }

    @Override
    public String getMetricName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public double getResult() {
        return getFscore();
    }
}
