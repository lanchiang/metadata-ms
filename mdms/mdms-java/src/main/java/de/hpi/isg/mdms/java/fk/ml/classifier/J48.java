package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A concrete J48 classifier.
 * @author Lan Jiang
 */
public class J48 extends AbstractClassifier {

    /**
     * The constructed decision tree.
     */
    private ClassifierTreeNode decisionTree;

    public J48() {
//        decisionTree = new ClassifierTreeNode();
    }

    @Override
    public void train() {
    }

    @Override
    public void predict() {

    }

    @Override
    public void buildClassifier(Dataset dataset) {

    }

    @Override
    public void classifyInstance(Instance instance) {

    }

    private void buildTree(Dataset trainSet, Set<Feature> featureSet) {
        // verify the trainset and featureset

        // initialize the root node
        decisionTree = new ClassifierTreeNode();
        if (trainSet.getNumOfClasses()==1) {
            decisionTree.setLeaf(true);
            decisionTree.setLeadLabel(trainSet.getDataset().get(0).getLabel());
            return;
        }

        if (featureSet.isEmpty()
                || trainSet.isSelfIndentity()) {
            decisionTree.setLeaf(true);
            decisionTree.setLeadLabel(trainSet.findMajority());
            return;
        }

        AttributeSelection attributeSelection = new AttributeSelection();
        Feature selectedFeature = attributeSelection.selectBestAttribute(trainSet, featureSet);

        Map<Object, List<Instance>> dsByFeatureValue = attributeSelection.splitDatasetByFeatureValue(trainSet, selectedFeature);
        for (Object value : dsByFeatureValue.keySet()) {
            ClassifierTreeNode treeNode = new ClassifierTreeNode();
        }
    }

    private void buildTree() {

    }
}
