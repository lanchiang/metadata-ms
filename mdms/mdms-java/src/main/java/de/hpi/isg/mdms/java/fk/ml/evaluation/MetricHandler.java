package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.Map;

/**
 * @author Lan Jiang
 */
public interface MetricHandler {

    public void calculateMetric(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth,
                                Map<UnaryForeignKeyCandidate, Instance.Result> predicted,
                                Instance.Result evaluatedLabel);
}
