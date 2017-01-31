package de.hpi.isg.mdms.java;

import de.hpi.isg.mdms.java.fk.Dataset;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;

import java.util.Random;

/**
 * Created by Fuga on 20/01/2017.
 */
public class J48W extends ClassifierW {

    public J48W(Dataset dataset) {
        this.dataset = dataset;
        convertData();
    }

    @Override
    public void buildClassifier() throws Exception {
        Evaluation eval = new Evaluation(data);

        J48 tree = new J48();
        eval.crossValidateModel(tree, data, 5, new Random(1));
        System.out.println(eval.toSummaryString("\nJ48 Results\n", false));
        System.out.println(eval.toClassDetailsString("\nJ48 Results\n"));
        System.out.println(eval.toMatrixString("\nJ48 Confusing Matrix\n"));
    }
}
