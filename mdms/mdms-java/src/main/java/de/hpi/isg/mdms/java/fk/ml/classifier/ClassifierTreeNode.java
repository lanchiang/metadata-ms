package de.hpi.isg.mdms.java.fk.ml.classifier;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.feature.Feature;

import java.util.List;

/**
 * @author Lan Jiang
 */
public class ClassifierTreeNode {

    /**
     * The feature included in this node.
     */
    private Feature feature;

    /**
     * The parent node of the current node in the classifier tree.
     */
    private ClassifierTreeNode parent;

    /**
     * The descendant nodes of the current node in the classifier tree.
     */
    private List<ClassifierTreeNode> descendants;

    /**
     * Whether the node is leaf.
     */
    private boolean isLeaf;

    /**
     * The label of the leaf node.
     */
    private Instance.Result leadLabel;

    /**
     * The branch condition of the current node.
     */
    BranchCondition branchCondition;

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void setLeadLabel(Instance.Result leadLabel) {
        this.leadLabel = leadLabel;
    }


}
