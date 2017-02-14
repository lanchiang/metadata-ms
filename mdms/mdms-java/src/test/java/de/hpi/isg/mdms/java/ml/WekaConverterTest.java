package de.hpi.isg.mdms.java.ml;

import de.hpi.isg.mdms.java.classifier.DecisionTableW;
import de.hpi.isg.mdms.java.classifier.J48W;
import de.hpi.isg.mdms.java.classifier.NaiveBayesW;
import de.hpi.isg.mdms.java.classifier.SVMW;
import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;
import de.hpi.isg.mdms.java.util.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.feature.CoverageFeature;
import de.hpi.isg.mdms.java.feature.DistinctDependentValuesFeature;
import de.hpi.isg.mdms.java.feature.Feature;
import de.hpi.isg.mdms.java.util.WekaConverter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fuga on 19/01/2017.
 */
public class WekaConverterTest {

    private static Dataset dataset;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        BufferedReader fakedDataReader = new BufferedReader(new FileReader
                ("/Users/Fuga/Documents/HPI/mdms/private-mdms/mdms/mdms-java/src/test/resource/faked-data.TXT"));

        String firstline = fakedDataReader.readLine();
        String[] attribute_names = firstline.split(",");
        List<Feature> features = new ArrayList<>();
        Feature feature = new CoverageFeature();
        feature.setFeatureName(attribute_names[0]);
        feature.setFeatureType(Feature.FeatureType.Numeric);
        features.add(feature);
        Feature feature1 = new DistinctDependentValuesFeature();
        feature1.setFeatureName(attribute_names[1]);
        feature1.setFeatureType(Feature.FeatureType.Numeric);
        features.add(feature1);


        String line;
        List<Instance> instances = new ArrayList<>();
        while ((line = fakedDataReader.readLine())!=null) {
            String[] info = line.split(",");
            Instance instance = new Instance(new UnaryForeignKeyCandidate(0,0));
            for (int i = 0; i<info.length-1; i++) {
                instance.setIsForeignKey((info[info.length-1].equals("yes")?
                        Instance.Result.FOREIGN_KEY: Instance.Result.NO_FOREIGN_KEY));
                instance.getFeatureVector().putIfAbsent(attribute_names[i], Double.parseDouble(info[i]));
            }
            instances.add(instance);
        }
        dataset = new Dataset(instances, features);
    }

    @Test
    public void testWriteToFile() throws Exception {
        WekaConverter wc = new WekaConverter(dataset, "dataset");
        wc.writeDataIntoFile();
    }

    @Test
    public void testBuildNaiveBayes() throws Exception {
        NaiveBayesW naiveBayesW = new NaiveBayesW(dataset);
        naiveBayesW.buildClassifier();
    }

    @Test
    public void testBuildJ48() throws Exception {
        J48W j48W = new J48W(dataset);
        j48W.buildClassifier();
    }

    @Test
    public void testBuildDecisionTable() throws Exception {
        DecisionTableW decisionTableW = new DecisionTableW(dataset);
        decisionTableW.buildClassifier();
    }

    @Test
    public void testBuildSVM() throws Exception {
        SVMW svmw = new SVMW(dataset);
        svmw.buildClassifier();
    }
}
