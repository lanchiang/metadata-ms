package de.hpi.isg.mdms.tools.metanome.reader;

import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.backend.result_receiver.ResultReceiver;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Parser for {@link de.metanome.algorithm_integration.results.FunctionalDependency} constraints.
 */
public class FunctionalDependencyReader extends AbstractResultReader<FunctionalDependency> {

    @Override
    protected void processLine(String line, ResultReceiver resultReceiver) {
        toFD(line).forEach(fd -> {
            try {
                resultReceiver.receiveResult(fd);
            } catch (CouldNotReceiveResultException e) {
                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
            }
        });
    }

    private Stream<FunctionalDependency> toFD(String line) {
        String cleanLine = line.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "");
        String[] split = cleanLine.split(" --> ");
        String[] rhsSplit = split[1].split(", ");
        ColumnCombination lhs = toColumnCombination(split[0]);
        return Arrays
                .stream(rhsSplit)
                .map(rhs -> new FunctionalDependency(lhs, toColumnIdentifier(rhs)));
    }
}
