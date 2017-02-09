package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Takes care of repeated parsing operations.
 */
public abstract class AbstractResultReader<T> implements ResultReader {

    public static final String TABLE_COLUMN_CONCATENATOR_ESC = "\\.";

    protected static final String TABLE_MARKER = "# TABLES";
    protected static final String COLUMN_MARKER = "# COLUMN";
    protected static final String RESULT_MARKER = "# RESULTS";

    protected static final String MAPPING_SEPARATOR = "\t";
    protected static final String IND_SEPARATOR_ESC = "\\[=";


    protected static ColumnCombination toColumnCombination(String line) {
        if (line.equals(""))
            return new ColumnCombination(); // Note: This is the empty set!
        String[] split = line.split(", ");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(s -> toColumnIdentifier(s)).collect(Collectors.toList());
        return new ColumnCombination(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnCombination toColumnCombination(Map<String, String> tableMapping,
                                                           Map<String, String> columnMapping, String line) {
        if (line.equals(""))
            return new ColumnCombination(); // Note: This is the empty set!
        String[] split = line.split(",");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(s -> getColumnIdentifier(tableMapping, columnMapping, s))
                .collect(Collectors.toList());
        return new ColumnCombination(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnPermutation toColumnPermutation(String line) {
        String[] split = line.split(",");
        List<ColumnIdentifier> identifiers = Arrays.stream(split).map(s -> toColumnIdentifier(s)).collect(Collectors.toList());
        return new ColumnPermutation(identifiers.toArray(new ColumnIdentifier[0]));
    }

    protected static ColumnIdentifier toColumnIdentifier(String line) {
        // If there are multiple separators, we rather believe, the table/column separator is the last one.
        int separatorPos = line.lastIndexOf('.');
        return new ColumnIdentifier(line.substring(0, separatorPos), line.substring(separatorPos + 1));
    }

    // used once
    protected static ColumnPermutation cpFromString(Map<String, String> tableMapping, Map<String, String> columnMapping, String str) {
        String[] parts = str.split(",");

        ColumnIdentifier[] identifiers = new ColumnIdentifier[parts.length];
        for (int i = 0; i < parts.length; i++) {
            identifiers[i] = AbstractResultReader.getColumnIdentifier(tableMapping, columnMapping, parts[i].trim());
        }
        return new ColumnPermutation(identifiers);
    }

    // used once
    public static ColumnIdentifier getColumnIdentifier(Map<String, String> tableMapping,
                                                       Map<String, String> columnMapping, String str) {
        if (str.isEmpty()) {
            return new ColumnIdentifier();
        }
        String[] parts = columnMapping.get(str).split(TABLE_COLUMN_CONCATENATOR_ESC, 2);
        String tableKey = parts[0];
        String columnName = parts[1];
        String tableName = tableMapping.get(tableKey);

        return new ColumnIdentifier(tableName, columnName);
    }

    @Override
    public void parse(final File resultFile, final ResultReceiver resultReceiver) {
        if (resultFile.exists()) {
            try {
                Files.lines(resultFile.toPath()).forEach(line -> processLine(line, resultReceiver));
            } catch (Exception e) {
                throw new RuntimeException("Could not parse " + resultFile, e);
            }
        } else {
            throw new RuntimeException("Could not find file at " + resultFile.getAbsolutePath());
        }
    }

    /**
     * Parse a line in the result file and puts the result into the {@code resultReceiver}.
     *
     * @param line           the line to parse
     * @param resultReceiver consumes the result
     */
    protected abstract void processLine(String line, ResultReceiver resultReceiver);

}
