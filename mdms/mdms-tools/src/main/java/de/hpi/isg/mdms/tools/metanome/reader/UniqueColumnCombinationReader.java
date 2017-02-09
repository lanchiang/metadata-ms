package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parser for {@link UniqueColumnCombination} constraints.
 */
public class UniqueColumnCombinationReader extends AbstractResultReader<UniqueColumnCombination> {

    private boolean isTableMapping = false;
    private boolean isColumnMapping = false;

    Map<String, String> tableMapping = new HashMap<>();
    Map<String, String> columnMapping = new HashMap<>();

    @Override
    protected void processLine(String line, ResultReceiver resultReceiver) {
        try {
            resultReceiver.receiveResult(toUCC(line));
        } catch (CouldNotReceiveResultException e) {
            throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
        }
    }

//    @Override
//    protected void processLine(String line, ResultReceiver resultReceiver) {
//        if (line.startsWith(TABLE_MARKER)) {
//            isTableMapping = true;
//            isColumnMapping = false;
//            return;
//        } else if (line.startsWith(COLUMN_MARKER)) {
//            isTableMapping = false;
//            isColumnMapping = true;
//            return;
//        } else if (line.startsWith(RESULT_MARKER)) {
//            isTableMapping = false;
//            isColumnMapping = false;
//            return;
//        }
//
//        if (isTableMapping) {
//            String[] parts = line.split(MAPPING_SEPARATOR);
//            if (parts[0].endsWith(".csv")) {
//                parts[0] = parts[0].substring(0, parts[0].length()-4);
//            }
//            tableMapping.put(parts[1],parts[0]);
//        } else if (isColumnMapping) {
//            String[] parts = line.split(MAPPING_SEPARATOR);
//            columnMapping.put(parts[1],parts[0]);
//        } else {
//            try {
//                resultReceiver.receiveResult(fromString(tableMapping, columnMapping, line));
////                System.out.println(line);
//            } catch (CouldNotReceiveResultException e) {
//                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
//            }
//        }
//    }

    private static UniqueColumnCombination fromString(Map<String, String> tableMapping,
                                                      Map<String, String> columnMapping, String str) {
        return new UniqueColumnCombination(AbstractResultReader.toColumnCombination(tableMapping, columnMapping, str));
    }

    private static UniqueColumnCombination toUCC(String line) {
        String cleanLine = line.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "");
        return new UniqueColumnCombination(toColumnCombination(cleanLine));
    }
}
