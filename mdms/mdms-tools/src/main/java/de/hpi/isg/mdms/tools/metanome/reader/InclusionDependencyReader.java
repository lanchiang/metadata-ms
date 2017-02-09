package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for {@link InclusionDependency} constraints.
 */
public class InclusionDependencyReader extends AbstractResultReader<InclusionDependency> {

    private Pattern rhsPattern = Pattern.compile("\\[([^\\[\\]]*)\\]");

    private boolean isTableMapping = false;
    private boolean isColumnMapping = false;

    Map<String, String> tableMapping = new HashMap<>();
    Map<String, String> columnMapping = new HashMap<>();

    @Override
    protected void processLine(String line, ResultReceiver resultReceiver) {
        if (line.startsWith(TABLE_MARKER)) {
            isTableMapping = true;
            isColumnMapping = false;
            return;
        } else if (line.startsWith(COLUMN_MARKER)) {
            isTableMapping = false;
            isColumnMapping = true;
            return;
        } else if (line.startsWith(RESULT_MARKER)) {
            isTableMapping = false;
            isColumnMapping = false;
            return;
        }

        if (isTableMapping) {
            String[] parts = line.split(MAPPING_SEPARATOR);
            if (parts[0].endsWith(".csv")) {
                parts[0] = parts[0].substring(0, parts[0].length()-4);
            }
            tableMapping.put(parts[1],parts[0]);
        } else if (isColumnMapping) {
            String[] parts = line.split(MAPPING_SEPARATOR);
            columnMapping.put(parts[1],parts[0]);
        } else {
            try {
                resultReceiver.receiveResult(fromString(tableMapping, columnMapping, line));
            } catch (CouldNotReceiveResultException e) {
                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
            }
        }
    }

//    @Override
//    protected void processLine(String line, ResultReceiver resultReceiver) {
//        toINDs(line).forEach(ind -> {
//            try {
//                resultReceiver.receiveResult(ind);
//            } catch (CouldNotReceiveResultException e) {
//                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
//            }
//        });
//    }

//    private Collection<InclusionDependency> toINDs(String line) {
//        String[] split = line.split(" c ");
//        ColumnPermutation lhs = toColumnPermutation(clean(split[0]));
//        String rhses = split[1];
//        Collection<InclusionDependency> inds = new LinkedList<>();
//        final Matcher matcher = this.rhsPattern.matcher(rhses);
//        while (matcher.find()) {
//            ColumnPermutation rhs = toColumnPermutation(matcher.group(1));
//            inds.add(new InclusionDependency(lhs, rhs));
//        }
//        return inds;
//    }

    public static InclusionDependency fromString(Map<String, String> tableMapping,
                                                 Map<String, String> columnMapping, String str) {
        String[] parts = str.split(IND_SEPARATOR_ESC);
        ColumnPermutation dependant = AbstractResultReader.cpFromString(tableMapping, columnMapping, parts[0]);
        ColumnPermutation referenced = AbstractResultReader.cpFromString(tableMapping, columnMapping, parts[1]);

        return new InclusionDependency(dependant,referenced);
    }

    private String clean(String src) {
        return src.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "");
    }
}
