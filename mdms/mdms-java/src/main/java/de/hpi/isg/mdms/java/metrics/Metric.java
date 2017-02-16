package de.hpi.isg.mdms.java.metrics;

import java.util.Arrays;
import java.util.function.Function;

/**
 * @author Lan Jiang
 * @since 15/02/2017
 */
abstract public class Metric {

    private double[][] confusionMatrix;

    private String labels;

    public double sumConfusionMatrix() {
        return Arrays.stream(confusionMatrix)
                .map(doubles -> Arrays.stream(doubles).sum())
                .mapToDouble(value -> value).sum();
    }

    public void setConfusionMatrix(double[][] confusionMatrix) {
        this.confusionMatrix = confusionMatrix;
    }

    public void setLabels(String labels) {
        this.labels = labels;
    }
}
