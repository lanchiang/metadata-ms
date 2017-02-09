package de.hpi.isg.mdms.tools.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.tools.metanome.ResultMetadataStoreWriter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class imports single column statistics from JSON files in a custom format from Hazar Harmouch.
 */
public class MetanomeStatisticsImportApp extends MdmsAppTemplate<MetanomeStatisticsImportApp.Parameters> {

    /**
     * Pattern to extract JSON keys for the top k frequent values.
     */
//    private static final Pattern TOP_K_FREQUENT_VALUES_PATTERN = Pattern.compile("Top (\\d+) frquent Items");
    private static final Pattern TOP_K_FREQUENT_VALUES_PATTERN = Pattern.compile("Frequency Of Top (\\d+) Frequent Items");

    /**
     * Pattern to generate JSON key for the top k frequent values.
     */
//    private static final String TOP_K_FREQUENT_VALUES_FORMAT = "Top %d frquent Items";
    private static final String TOP_K_FREQUENT_VALUES_FORMAT = "Top %d frequent items";

    private static final String FREQUENCY_TOP_K_ITEMS_FORMAT = "Frequency Of Top %d Frequent Items";

    /**
     * Key used in the {@link #executionMetadata} to present the ID of the generated constraint collection.
     */
    public static final String CONSTRAINT_COLLECTION_ID_KEY = "constraintCollectionId";

    public static void main(String[] args) throws Exception {
        MetanomeStatisticsImportApp.Parameters parameters = new MetanomeStatisticsImportApp.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new MetanomeStatisticsImportApp(parameters).run();
    }

    public MetanomeStatisticsImportApp(MetanomeStatisticsImportApp.Parameters parameters) {
        super(parameters);
    }

    public static void fromParameters(MetadataStore mds, String fileLocation, String schemaName) throws Exception {

        MetanomeStatisticsImportApp.Parameters parameters = new MetanomeStatisticsImportApp.Parameters();

        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(fileLocation);
        parameters.inputDirectories = inputFiles;
        parameters.schemaName = schemaName;

        MetanomeStatisticsImportApp app = new MetanomeStatisticsImportApp(parameters);
        app.metadataStore = mds;

        app.run();
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Set up the facilities to write to the metadata store.
        Schema schema = this.metadataStore.getSchemaByName(this.parameters.schemaName);
        if (schema == null) {
            throw new IllegalArgumentException("No such schema: " + this.parameters.schemaName);
        }
        ConstraintCollection constraintCollection = this.metadataStore.createConstraintCollection(this.parameters.getDescription(), schema);


        for (String inputDirectoryPath : this.parameters.inputDirectories) {

            // Discover files with statistics.
            Map<File, String> fileToTableNameMap = discoverStatisticsFiles(inputDirectoryPath);

            // Now import all the files.
            for (Map.Entry<File, String> fileToTableNameEntry : fileToTableNameMap.entrySet()) {
                final File statisticsFile = fileToTableNameEntry.getKey();
//                final String tableName = fileToTableNameEntry.getValue();
                final String tableName = fileToTableNameEntry.getValue()+".csv";

                // Find the table in the schema.
                final Table table = schema.getTableByName(tableName);
                if (table == null) {
                    getLogger().warn("Could not find the table {} in the metadata store. Skipping...", tableName);
                    continue;
                }

                // Load the file.
                List<String> lines;
                try {
                    lines = Files
                            .lines(statisticsFile.toPath(), Charset.forName(this.parameters.encoding))
                            .collect(Collectors.toList());
                } catch (Exception e) {
                    getLogger().error("Could not read " + statisticsFile + ".", e);
                    continue;
                }

                // The first line contains general data about the profiled table.
//                final String firstLine = lines.get(0);
//                final long numTuples = processFirstStatisticsFileLine(firstLine, table, constraintCollection);

                int numTuples = 0;
                for (String columnStatisticsLine : lines) {
                    try {
                        final JSONObject columnStatisticsObject = new JSONObject(columnStatisticsLine);
                        final int numberDistinctValue = columnStatisticsObject.getJSONObject("statisticMap")
                                .getJSONObject("Number of Distinct Values").getInt("value");
                        if (numTuples < numberDistinctValue) {
                            numTuples = numberDistinctValue;
                        }
                    } catch (Exception e) {
                        getLogger().error("Could not handle " + columnStatisticsLine + ".", e);
                    }
                }
                // All following lines contain statistics on different columns.
                for (String columnStatisticsLine : lines) {

                    try {
                        final JSONObject columnStatisticsObject = new JSONObject(columnStatisticsLine);
//                        final String metanomeColumnIdentifier = columnStatisticsObject.getString("column Name");
                        final JSONArray columnIdentifierArr = columnStatisticsObject
                                .getJSONObject("columnCombination")
                                .getJSONArray("columnIdentifiers");
//                        final String tableName = columnIdentifier.getString("tableIdentifier");
                        final String columnName = ResultMetadataStoreWriter
                                .convertMetanomeColumnIdentifier(columnIdentifierArr.getJSONObject(0).getString("columnIdentifier"));

                        final Column column = table.getColumnByName(columnName);
                        if (column == null) {
                            getLogger().warn("Could not find the column {} in table {}. Skipping...", columnName, table.getName());
                            continue;
                        }
                        final JSONObject statisticMapObject = columnStatisticsObject.getJSONObject("statisticMap");
                        extractGeneralColumnStatistics(statisticMapObject, column, constraintCollection, numTuples);


                        // For numeric columns, create a specific statistics object.
                        extractNumberColumnStatistics(constraintCollection, statisticMapObject, column);

                        // For character columns, create a specific statistics object.
                        extractTextColumnStatistics(constraintCollection, statisticMapObject, column);
                    } catch (Exception e) {
                        getLogger().error("Could not handle " + columnStatisticsLine + ".", e);
                    }
                }

            }
        }

        // Finalize.
        this.metadataStore.close();

        this.executionMetadata.addCustomData(CONSTRAINT_COLLECTION_ID_KEY, constraintCollection.getId());
    }

    /**
     * Extracts statistics that are specific to text columns.
     *
     * @param constraintCollection   stores any created constraints
     * @param columnStatisticsObject input statistics of the column
     * @param column                 the column described by the input statistics
     */
    private void extractTextColumnStatistics(ConstraintCollection constraintCollection, JSONObject columnStatisticsObject, Column column) {
        if (columnStatisticsObject.has("Min String")) {
            TextColumnStatistics textColumnStatistics = new TextColumnStatistics(column.getId());
            textColumnStatistics.setMinValue(columnStatisticsObject.getJSONObject("Min String").getString("value"));
            textColumnStatistics.setMaxValue(columnStatisticsObject.getJSONObject("Max String").getString("value"));
            textColumnStatistics.setShortestValue(columnStatisticsObject.getJSONObject("Shortest String").getString("value"));
            textColumnStatistics.setLongestValue(columnStatisticsObject.getJSONObject("Longest String").getString("value"));
            if (columnStatisticsObject.has("Symantic Data Type")) {
                textColumnStatistics.setSubtype(columnStatisticsObject.getJSONObject("Symantic Data Type").getString("value"));
            }
            constraintCollection.add(textColumnStatistics);
        }
    }


    /**
     * Extracts statistics that are specific to numeric columns.
     *
     * @param constraintCollection   stores any created constraints
     * @param columnStatisticsObject input statistics of the column
     * @param column                 the column described by the input statistics
     */
    private void extractNumberColumnStatistics(ConstraintCollection constraintCollection, JSONObject columnStatisticsObject, Column column) {
        if (columnStatisticsObject.has("Min")) {
            NumberColumnStatistics numberColumnStatistics = new NumberColumnStatistics(column.getId());
            numberColumnStatistics.setMinValue(columnStatisticsObject.getJSONObject("Min").getDouble("value"));
            numberColumnStatistics.setMaxValue(columnStatisticsObject.getJSONObject("Max").getDouble("value"));
            numberColumnStatistics.setAverage(columnStatisticsObject.getJSONObject("Avg.").getDouble("value"));
            if (columnStatisticsObject.has("Standard Deviation")) {
                numberColumnStatistics.setStandardDeviation(columnStatisticsObject.getJSONObject("Standard Deviation").getDouble("value"));
            }
            constraintCollection.add(numberColumnStatistics);
        }
    }


    /**
     * Extracts statistics that apply to all columns.
     *
     * @param constraintCollection   stores any created constraints
     * @param columnStatisticsObject input statistics of the column
     * @param column                 the column described by the input statistics
     * @param numTuples              number of tuples in the table that contains the column
     */
    private void extractGeneralColumnStatistics(JSONObject columnStatisticsObject, Column column,
                                                ConstraintCollection constraintCollection,
                                                long numTuples) {
        // Harvest the column type.
        final String dataType = columnStatisticsObject.getJSONObject("Data Type").getString("value");
        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(column.getId()), constraintCollection, dataType);

        // Harvest the general column statistics.
        ColumnStatistics columnStatistics = new ColumnStatistics(column.getId());
        columnStatistics.setNumNulls(columnStatisticsObject.getJSONObject("Nulls").getLong("value"));
        columnStatistics.setFillStatus(1d - columnStatisticsObject.getJSONObject("Percentage of Nulls").getDouble("value"));
        columnStatistics.setNumDistinctValues(columnStatisticsObject.getJSONObject("Number of Distinct Values").getLong("value"));

        // Some statistics can only exist if there are any values at all.
        long numNonNulls = numTuples - columnStatistics.getNumNulls();
        if (numNonNulls > 0L) {
            // Calculate the uniqueness, which is not imported but derived from the statistics.
            columnStatistics.setUniqueness(columnStatistics.getNumDistinctValues() / (double) numNonNulls);

            // Find the most frequent values.
            final OptionalInt maxK = columnStatisticsObject.keySet().stream()
                    .flatMapToInt((key) -> {
                        final Matcher matcher = TOP_K_FREQUENT_VALUES_PATTERN.matcher(key);
                        if (matcher.matches()) {
                            return IntStream.of(Integer.valueOf(matcher.group(1)));
                        } else {
                            return IntStream.empty();
                        }
                    })
                    .max();
            if (maxK.isPresent()) {
                String items = String.format(TOP_K_FREQUENT_VALUES_FORMAT, maxK.getAsInt());
                String frequency = String.format(FREQUENCY_TOP_K_ITEMS_FORMAT, maxK.getAsInt());
                final JSONObject topKFrequentValuesObject = columnStatisticsObject.getJSONObject(items);
                final JSONObject topKFrequncyObject = columnStatisticsObject.getJSONObject(frequency);
                Map<String, Long> itemFrequnceyMap = new HashMap<>();
                JSONArray itemsArray = topKFrequentValuesObject.getJSONArray("value");
                JSONArray frequencyArray = topKFrequncyObject.getJSONArray("value");
                for (int i = 0; i < itemsArray.length(); i++) {
                    itemFrequnceyMap.putIfAbsent(itemsArray.getString(i), frequencyArray.getLong(i));
                }

                final List<ColumnStatistics.ValueOccurrence> topKEntryList = itemFrequnceyMap.keySet().stream()
                        .map(topKValue -> {
                            return new ColumnStatistics.ValueOccurrence(topKValue, itemFrequnceyMap.get(topKValue));
                        })
                        .sorted(Collections.reverseOrder())
                        .collect(Collectors.toList());
                columnStatistics.setTopKFrequentValues(topKEntryList);
            }
        }

        // Save the statistics.
        constraintCollection.add(columnStatistics);
    }

    /**
     * Extract the information that is found in the first line of statistics files.
     *
     * @param firstLine            the raw input line
     * @param table                the table that is described by the statistics file
     * @param constraintCollection stores any new constraints
     * @return the number of tuples found in the statics file
     */
    private long processFirstStatisticsFileLine(String firstLine, Table table, ConstraintCollection constraintCollection) {
        final JSONObject firstLineObject = new JSONObject(firstLine);
        final long numTuples = firstLineObject.getLong("# Tuples");
        TupleCount.buildAndAddToCollection(new SingleTargetReference(table.getId()), constraintCollection, (int) numTuples);
        return numTuples;
    }

    /**
     * Discovers the statistics files in the given directory.
     *
     * @param inputDirectoryPath a directory with statistics files
     * @return a mapping from statistics files to the names of the tables that they describe
     */
    private Map<File, String> discoverStatisticsFiles(String inputDirectoryPath) {
        // Detect files to import.
        File inputDir = new File(inputDirectoryPath);
        if (!inputDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + inputDir);
        }
        Pattern statisticsFilePattern = Pattern.compile("(.+)_SingleColumnProfiler\\.txt");
        final File[] staticsFiles = inputDir.listFiles((file) -> statisticsFilePattern.matcher(file.getName()).matches());
        return Arrays.stream(staticsFiles).collect(Collectors.toMap(
                Function.identity(),
                (file) -> {
                    final Matcher matcher = statisticsFilePattern.matcher(file.getName());
                    if (!matcher.matches()) throw new IllegalStateException();
                    return matcher.group(1);
                }));
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected boolean isCleanUpRequested() {
        return false;
    }

    /**
     * Parameters for {@link MetanomeStatisticsImportApp}.
     */
    public static class Parameters {

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(description = "directories with single column statistics files",
                required = true)
        public List<String> inputDirectories;

        @Parameter(names = "--description",
                description = "description for the imported constraint collection",
                required = false)
        public String description;

        @Parameter(names = "--encoding",
                description = "encoding of the statistics files",
                required = false)
        public String encoding = "ISO-8859-1";

        /**
         * @return the user-specified description or a default one
         */
        public String getDescription() {
            return this.description == null ? "Single column statistics import" : this.description;
        }

        @Parameter(names = MetadataStoreParameters.SCHEMA_NAME,
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION,
                required = true)
        public String schemaName;

    }

}
