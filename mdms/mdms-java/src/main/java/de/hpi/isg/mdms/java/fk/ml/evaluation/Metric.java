package de.hpi.isg.mdms.java.fk.ml.evaluation;


/**
 * @author Lan Jiang
 */
abstract public class Metric implements MetricHandler {

    private String metricName;

    private double result;

    abstract public String getMetricName();

    abstract public double getResult();
}
