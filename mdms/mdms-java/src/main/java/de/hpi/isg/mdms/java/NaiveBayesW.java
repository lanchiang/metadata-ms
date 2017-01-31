package de.hpi.isg.mdms.java;

import de.hpi.isg.mdms.java.fk.Dataset;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.SMO;

import java.util.Random;

/**
 * Created by Fuga on 19/01/2017.
 */
public class NaiveBayesW extends ClassifierW {

    public NaiveBayesW(Dataset dataset) {
        this.dataset = dataset;
        convertData();
    }

    @Override
    public void buildClassifier() throws Exception {
        Evaluation eval = new Evaluation(data);

//        cls = new NaiveBayes();
//        cls.buildClassifier(data);
//        System.out.println(eval.toSummaryString("\nNaive Bayes Results\n", true));
        NaiveBayes naiveBayes = new NaiveBayes();
        eval.crossValidateModel(naiveBayes, data, 5, new Random(1));
        System.out.println(eval.toSummaryString("\nNaiveBayes Results\n", false));
        System.out.println(eval.toClassDetailsString("\nNaiveBayes Results\n"));
        System.out.println(eval.toMatrixString("\nNaive Bayes Confusing Matrix\n"));
    }
}
