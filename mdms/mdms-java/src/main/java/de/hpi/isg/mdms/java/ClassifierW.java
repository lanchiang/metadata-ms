package de.hpi.isg.mdms.java;

import de.hpi.isg.mdms.java.fk.Dataset;
import weka.classifiers.AbstractClassifier;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

import java.io.File;
import java.io.IOException;

/**
 * Created by Fuga on 20/01/2017.
 */
abstract public class ClassifierW {

    protected Dataset dataset;

    protected WekaConverter wekaConverter;

    protected Instances data;

    protected AbstractClassifier cls;

    /**
     * Convert the instances into arff format.
     */
    public void convertData() {
        try {
            wekaConverter = new WekaConverter();
            wekaConverter.writeDataIntoFile(dataset);
            String fileName = wekaConverter.getFileName();
            ArffLoader loader = new ArffLoader();
            loader.setFile(new File(fileName));
            data = loader.getDataSet();
            data.setClassIndex(data.numAttributes()-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract public void buildClassifier() throws Exception;
}
