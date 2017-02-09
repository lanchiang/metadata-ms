package de.hpi.isg.mdms.java.sampling;

import de.hpi.isg.mdms.java.util.Dataset;
import de.hpi.isg.mdms.java.util.Instance;

import java.util.List;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 09/02/2017
 */
abstract public class Undersampling {

    /**
     * The {@link Dataset} that need the under sampling.
     */
    protected Dataset dataset;

    /**
     * Pointing out the majority and minority classes in this {@link Dataset}.
     */
    protected Instance.Result majorityClass;
    protected Instance.Result minorityClass;

    /**
     * Split the {@link Dataset} by the label information.
     */
    protected Map<Instance.Result, List<Instance>> instanceByClasses;


}
