package de.hpi.isg.mdms.java;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.bayes.NaiveBayesUpdateable;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Created by Fuga on 19/01/2017.
 */
public class WekaTest {

    public static void main(String[] args) throws Exception {
        Instances data = DataSource.read("/Users/Fuga/Documents/weka-3-8-0/data/breast-cancer.arff");
        data.setClassIndex(data.numAttributes() - 1);
        Evaluation eval = new Evaluation(data);
//        J48 tree = new J48();
        NaiveBayes tree = new NaiveBayes();
        eval.crossValidateModel(tree, data, 10, new Random(1));
        System.out.println(eval.toSummaryString("\nResults\n\n", false));
    }
}
