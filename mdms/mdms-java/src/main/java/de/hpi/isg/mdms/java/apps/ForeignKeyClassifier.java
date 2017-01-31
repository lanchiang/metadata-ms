package de.hpi.isg.mdms.java.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.RDBMSMetadataStore;
import de.hpi.isg.mdms.domain.constraints.ColumnStatistics;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.TupleCount;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.domain.util.SQLiteConstraintUtils;
import de.hpi.isg.mdms.java.DecisionTableW;
import de.hpi.isg.mdms.java.J48W;
import de.hpi.isg.mdms.java.NaiveBayesW;
import de.hpi.isg.mdms.java.SVMW;
import de.hpi.isg.mdms.java.fk.Dataset;
import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;
import de.hpi.isg.mdms.java.fk.feature.*;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import de.hpi.isg.mdms.tools.metanome.ResultMetadataStoreWriter;
import it.unimi.dsi.fastutil.ints.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This app tries to distinguish actual foreign keys among a set of inclusion dependencies. For this purpose, it takes
 * as input a {@link ConstraintCollection} of
 * {@link InclusionDependency} and derives from that a new one that only contains
 * the classified foreign keys.
 */
public class ForeignKeyClassifier extends MdmsAppTemplate<ForeignKeyClassifier.Parameters> {

    private List<Feature> features = new ArrayList<>();

    public ForeignKeyClassifier(final ForeignKeyClassifier.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }


    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        SQLiteConstraintUtils.registerStandardConstraints(
                (SQLiteInterface) ((RDBMSMetadataStore) this.metadataStore).getSQLInterface());
    }

    @Override
    protected void executeAppLogic() throws Exception {

//        Set<UnaryForeignKeyCandidate> fkSet = readFkFromFile();

        // Load all relevant constraint collections.
        getLogger().info("Loading FKs...");
        final ConstraintCollection fkCollection = this.metadataStore.getConstraintCollection(this.parameters.fkCollectionId);
        getLogger().info("Loading INDs...");
        final ConstraintCollection indCollection = this.metadataStore.getConstraintCollection(this.parameters.indCollectionId);
        getLogger().info("Loading DVCs...");
        final ConstraintCollection dvcCollection = this.metadataStore.getConstraintCollection(this.parameters.dvcCollectionId);
        getLogger().info("Loading UCCs...");
        final ConstraintCollection uccCollection = this.metadataStore.getConstraintCollection(this.parameters.uccCollectionId);
        getLogger().info("Loading statistics...");
        final ConstraintCollection statsCollection = this.metadataStore.getConstraintCollection(this.parameters.statisticsCollectionId);

        // Collect all not-null columns.
        getLogger().info("Detecting not-null columns...");
        IntSet nullableColumns = new IntOpenHashSet();
        statsCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof ColumnStatistics)
                .map(constraint -> (ColumnStatistics) constraint)
                .filter(columnStatistics -> columnStatistics.getNumNulls() > 0)
                .forEach(columnStatistics -> nullableColumns.add(columnStatistics.getTargetReference().getTargetId()));

        // Find PK candidates (by removing UCCs with nullable columns) and index them by their table.
        getLogger().info("Identifying PK candidates...");
        final IdUtils idUtils = this.metadataStore.getIdUtils();
        final Map<Integer, List<UniqueColumnCombination>> tableUccs = uccCollection.getConstraints().stream()
                .filter(constraint -> constraint instanceof UniqueColumnCombination)
                .map(constraint -> (UniqueColumnCombination) constraint)
                .filter(ucc -> ucc.getTargetReference().getAllTargetIds().stream().noneMatch(nullableColumns::contains))
                .collect(Collectors.groupingBy(
                        ucc -> idUtils.getTableId(ucc.getTargetReference().getAllTargetIds().iterator().nextInt()),
                        Collectors.toList()));

        final IntSet nonEmptyTableIds = new IntOpenHashSet();
        if (this.parameters.isNeglectEmptyTables) {
            getLogger().info("Loading tuple counts...");
            statsCollection.getConstraints().stream()
                    .filter(constraint -> constraint instanceof TupleCount)
                    .map(constraint -> (TupleCount) constraint)
                    .filter(tupleCount -> tupleCount.getNumTuples() > 0)
                    .forEach(tupleCount -> nonEmptyTableIds.add(tupleCount.getTargetReference().getTargetId()));
            getLogger().info("Found {} non-empty tables.", nonEmptyTableIds.size());
        }

        // Keep only those INDs that reference primary key (candidates).
        getLogger().info("Filter PK-referencing, non-empty INDs...");
        final List<InclusionDependency> relevantInds = indCollection.getConstraints().stream()
                .map(constraint -> (InclusionDependency) constraint)
                .filter(ind -> {
                    if (!this.parameters.isNeglectEmptyTables) return true;
                    final int depTableId = idUtils.getTableId(ind.getTargetReference().getDependentColumns()[0]);
                    final int refTableId = idUtils.getTableId(ind.getTargetReference().getReferencedColumns()[0]);
                    return nonEmptyTableIds.contains(depTableId) && nonEmptyTableIds.contains(refTableId);
                })
                .filter(ind -> {
                    final int[] refColumnIds = ind.getTargetReference().getReferencedColumns();
                    final int tableId = idUtils.getTableId(refColumnIds[0]);
                    final List<UniqueColumnCombination> uccs = tableUccs.get(tableId);
                    return uccs != null && uccs.stream()
                            .anyMatch(ucc -> uccIsReferenced(ucc, ind, this.parameters.isWithUccSupersets));
                })
                .collect(Collectors.toList());
        getLogger().info("Detected {} relevant INDs from {} INDs overall.", relevantInds.size(), indCollection.getConstraints().size());

        final Set<UnaryForeignKeyCandidate> fkSet = fkCollection.getConstraints().stream()
                .map(constraint -> (InclusionDependency)constraint)
                .filter(ind -> {
                    int depCount = ind.getTargetReference().getDependentColumns().length;
                    return depCount==1?true:false;
                })
                .map(ind -> {
                    int[] dep = ind.getTargetReference().getDependentColumns();
                    int[] ref = ind.getTargetReference().getReferencedColumns();
                    return new UnaryForeignKeyCandidate(dep[0],ref[0]);
                })
                .collect(Collectors.toSet());

        List<Instance> instances = relevantInds.stream()
                .flatMap(this::splitIntoUnaryForeignKeyCandidates)
                .distinct()
                .map(Instance::new)
                .collect(Collectors.toList());

        this.features.add(new CoverageFeature(statsCollection));
        this.features.add(new DependentAndReferencedFeature());
        this.features.add(new DistinctDependentValuesFeature(statsCollection));
        this.features.add(new MultiDependentFeature());
        this.features.add(new MultiReferencedFeature());

        Dataset dataset = new Dataset(instances, features);
        dataset.buildFeatureValueDistribution();
        dataset.labelDataset(fkSet);

        NaiveBayesW naiveBayesW = new NaiveBayesW(dataset);
        naiveBayesW.buildClassifier();

        J48W j48W = new J48W(dataset);
        j48W.buildClassifier();

        SVMW svmw = new SVMW(dataset);
        svmw.buildClassifier();

        DecisionTableW decisionTableW = new DecisionTableW(dataset);
        decisionTableW.buildClassifier();
    }

    /**
     * Tells whether an {@link InclusionDependency} references a {@link UniqueColumnCombination}.
     *
     * @param ucc                 a {@link UniqueColumnCombination}
     * @param inclusionDependency a {@link InclusionDependency}
     * @param isWithUccSupersets  if referencing a superset of the {@code ucc} is also allowed
     * @return whether the referencing relationship exists
     */
    private boolean uccIsReferenced(UniqueColumnCombination ucc, InclusionDependency inclusionDependency,
                                    boolean isWithUccSupersets) {
        final IntCollection uccColumnIds = ucc.getTargetReference().getAllTargetIds();
        final int[] refColumnIds = inclusionDependency.getTargetReference().getReferencedColumns();

        // Conclude via the cardinalities of the UCC and IND.
        if (uccColumnIds.size() > refColumnIds.length || (!isWithUccSupersets && uccColumnIds.size() < refColumnIds.length)) {
            return false;
        }

        // Make a brute-force inclusion check.
        return uccColumnIds.stream().allMatch(uccColumnId -> arrayContains(refColumnIds, uccColumnId));
    }

    private boolean arrayContains(int[] array, int element) {
        for (int i : array) {
            if (i == element) return true;
        }
        return false;
    }

    /**
     * Splits a given {@link InclusionDependency} into {@link UnaryForeignKeyCandidate}s.
     *
     * @param ind the {@link InclusionDependency} to split
     * @return the {@link UnaryForeignKeyCandidate}s
     */
    private Stream<UnaryForeignKeyCandidate> splitIntoUnaryForeignKeyCandidates(InclusionDependency ind) {
        List<UnaryForeignKeyCandidate> fkCandidates = new ArrayList<>(ind.getArity());
        final int[] depColumnIds = ind.getTargetReference().getDependentColumns();
        final int[] refColumnIds = ind.getTargetReference().getReferencedColumns();
        for (int i = 0; i < ind.getArity(); i++) {
            fkCandidates.add(new UnaryForeignKeyCandidate(depColumnIds[i], refColumnIds[i]));
        }
        return fkCandidates.stream();
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    private Set<UnaryForeignKeyCandidate> readFkFromFile() {
        Set<UnaryForeignKeyCandidate> fkcandidates = new HashSet<>();
        try {
            Pattern pattern = Pattern.compile("\\[([^\\[\\]]*)\\]");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(this.parameters.evaluationFile));
            String line;
            int count = 0;
            while ((line=bufferedReader.readLine())!=null) {
                String[] info = line.split(" c ");
                Matcher matcher = pattern.matcher(info[0]);
                String lhs = "";
                while (matcher.find()) {
                    lhs = matcher.group(1);
                }
                matcher = pattern.matcher(info[1]);
                String rhs = "";
                while (matcher.find()) {
                    rhs = matcher.group(1);
                }
                int lhsId = getColId(lhs);
                int rhsId = getColId(rhs);
                if (lhsId!=-1&&rhsId!=-1) {
                    fkcandidates.add(new UnaryForeignKeyCandidate(lhsId, rhsId));
                }
                System.out.println(count++);
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
//        } finally {
//            return fkcandidates;
        }
        return fkcandidates;
    }

    private int getColId(String line) {
        String[] split = line.split("\\.");
        String columnName = null;
        String tableName = null;
        columnName = ResultMetadataStoreWriter.convertMetanomeColumnIdentifier(split[split.length-1]);
        if (split.length==2) {
            tableName = split[0];
        }
        else {
            tableName = String.join(",", split[0], split[1]);
        }
        Table table = this.metadataStore.getSchemaByName(this.parameters.schemaName)
                .getTableByName(tableName);
        if (table==null) {
            return -1;
        }
        int columnId = table.getColumnByName(columnName).getId();
        return columnId;
    }

    /**
     * Parameters for the {@link ForeignKeyClassifier} app.
     */
    public static class Parameters {

        @Parameter(names = {"--inds"},
                description = "ID of the constraint collection that contains the input INDs",
                required = true)
        public int indCollectionId;

        @Parameter(names = {"--distinct-values"},
                description = "ID of the constraint collection that contains the distinct value counts",
                required = false)
        public int dvcCollectionId;

        @Parameter(names = {"--fks"},
                description = "ID of the constraint collection that contains the input FKs",
                required = true)
        public int fkCollectionId;

        @Parameter(names = {"--statistics"},
                description = "ID of the constraint collection that contains single column statistics",
                required = true)
        public int statisticsCollectionId;

        @Parameter(names = {"--uccs"},
                description = "ID of the constraint collection that contains unique column combinations",
                required = true)
        public int uccCollectionId;

        @Parameter(names = {"--with-ucc-supersets"},
                description = "supersets of minimal UCCs are not considered PK candidates",
                required = false)
        public boolean isWithUccSupersets;

        @Parameter(names = {"--min-fk-score"},
                description = "minimum score for a foreign key candidate to be considered a foreign key",
                required = false)
        public double minFkScore = 0.1d;

        @Parameter(names = "--dry-run",
                description = "do not create a constraint collection for the foreign keys",
                required = false)
        public boolean isDryRun = false;

//        @Parameter(names = "--evaluation-files",
//                description = "table definition file and FK definition file",
//                arity = 2,
//                required = false)
//        public List<String> evaluationFiles = new ArrayList<>(2);

        @Parameter(names = "--evaluation-files",
                description = "name of the evaluation file",
                required = false)
        public String evaluationFile = null;

        @Parameter(names = "--schema-name",
                description = "schema name for evaluation purposes",
                required = false)
        public String schemaName = null;

        @Parameter(names = {"--no-empty-tables"},
                description = "do not consider empty tables in the calculation of precision and recall",
                required = false)
        public boolean isNeglectEmptyTables = false;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();
    }

    public static void main(String[] args) throws Exception {
        ForeignKeyClassifier.Parameters parameters = new ForeignKeyClassifier.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new ForeignKeyClassifier(parameters).run();
    }
}
