package de.hpi.isg.mdms.java.metrics;

import weka.core.matrix.Matrix;

import java.util.Arrays;
import java.util.Map;

/**
 * The concrete class for confusion matrix.
 * @author Lan Jiang
 * @since 16/02/2017
 */
public class ConfusionMatrix extends Matrix {

    /**
     * store the label names in the same order with the index in the confusion matrix.
     */
    private Map<String, Integer> labels;

    public ConfusionMatrix(int m, int n, Map<String, Integer> labels) {
        super(m, n);
        this.labels = labels;
    }

    /**
     * calculate the sum of all the elements in the matrix.
     * @return the sum of all the elements in the matrix
     */
    public double sumConfusionMatrix() {
        return Arrays.stream(this.getArray())
                .map(doubles -> Arrays.stream(doubles).sum())
                .mapToDouble(value -> value).sum();
    }

    /**
     * update the confusion matrix when a {@link weka.core.Instance} is classified.
     *
     * @param predictedLabel is the predicted label for the {@link weka.core.Instance}
     * @param actualLabel    is the actual label of the {@link weka.core.Instance}
     */
    public void updateConfusionMatrix(String predictedLabel, String actualLabel) {
        double currentCount = A[labels.get(actualLabel)][labels.get(predictedLabel)];
        A[labels.get(actualLabel)][labels.get(predictedLabel)] = currentCount + 1;
    }

    public double[][] getConfusionMatrix() {
        return super.getArray();
    }

    public void setConfusionMatrix(double[][] confusionMatrix) {
        super.A = confusionMatrix;
    }

    public Map<String, Integer> getLabels() {
        return labels;
    }
}