package de.hpi.isg.mdms.java.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import de.hpi.isg.mdms.java.metrics.GeometricMean;
import de.hpi.isg.mdms.java.metrics.Metric;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lan Jiang
 * @since 15/02/2017
 */
public class MetricTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private double[][] confusionMatrix;

    private String labels;

    Metric metric;

    @Before
    public void setUp() throws Exception {
        confusionMatrix = new double[][]{{1,2,3},{3,4,5},{3,2,3}};
//        metric = new GeometricMean(null);
    }

    @After
    public void tearDown() throws Exception {


    }

    @Test
    public void testCreateMetric() throws Exception {

        if (metric == null) {
            logger.info("The metric " + metric.getClass().getName() + " is not created successfully");
            System.exit(1);
        }
    }

//    @Test
//    public void testSumConfusionMatrix() throws Exception {
//        Metric metric = new GeometricMean(null);
//        metric.setConfusionMatrix(confusionMatrix);
//
//        assertEquals(26.0, metric.sumConfusionMatrix(), 0);
//    }
//
//    @Test
//    public void testGetTraceOfMatrix() throws Exception {
//        Metric metric = new GeometricMean(null);
//        metric.setConfusionMatrix(confusionMatrix);
//
//        assertEquals(8.0, metric.getTraceOfConfusionMatrix(), 0);
//    }
//
//    @Test
//    public void testGetGeometricMean() throws Exception {
//        Metric metric = new GeometricMean(null);
//        metric.setConfusionMatrix(confusionMatrix);
//
//        if (metric instanceof GeometricMean) {
//            assertEquals(Math.sqrt(1.0/48.0), ((GeometricMean) metric).getGeometricMean(), 0);
//        }
//    }
}
