package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;

/**
 * This interface represent the operations that a concrete classifier may have, i.e. train and predict.
 * @author Lan Jiang
 */
public interface Classifier {

    void train();

    void predict();

    void buildClassifier(Dataset dataset);

    void classifyInstance(Instance instance);
}
