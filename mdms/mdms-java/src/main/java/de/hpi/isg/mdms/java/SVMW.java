package de.hpi.isg.mdms.java;

import de.hpi.isg.mdms.java.fk.Dataset;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.SMO;
import weka.classifiers.pmml.consumer.SupportVectorMachineModel;
import weka.core.pmml.jaxbbindings.SupportVectorMachine;

import java.util.Random;

/**
 * Created by Fuga on 20/01/2017.
 */
public class SVMW extends ClassifierW {

    public SVMW(Dataset dataset) {
        this.dataset = dataset;
        try {
            convertData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void buildClassifier() throws Exception {
        Evaluation eval = new Evaluation(data);

        SMO svm = new SMO();
        eval.crossValidateModel(svm, data, 5, new Random(1));
        System.out.println(eval.toSummaryString("\nSVM Results\n", false));
        System.out.println(eval.toClassDetailsString("\nSVM Results\n"));
        System.out.println(eval.toMatrixString("\nSVM Confusing Matrix\n"));
    }
}
