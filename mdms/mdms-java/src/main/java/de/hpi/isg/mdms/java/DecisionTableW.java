package de.hpi.isg.mdms.java;

import de.hpi.isg.mdms.java.fk.Dataset;
import weka.classifiers.Evaluation;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.trees.J48;

import java.util.Random;

/**
 * Created by Fuga on 20/01/2017.
 */
public class DecisionTableW extends ClassifierW{

    public DecisionTableW(Dataset dataset) {
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

        DecisionTable decisionTable = new DecisionTable();
        eval.crossValidateModel(decisionTable, data, 5, new Random(1));
        System.out.println(eval.toSummaryString("\nDecisionTable Results\n", false));
        System.out.println(eval.toClassDetailsString("\nDecisionTable Results\n"));
        System.out.println(eval.toMatrixString("\nDecision Table Confusing Matrix\n"));
    }
}
