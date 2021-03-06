package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * This constraint class encapsulates string-specific single column statistics.
 */
public class TextColumnStatistics implements RDBMSConstraint {

    /**
     * Special values in a column.
     */
    private String minValue, maxValue, shortestValue, longestValue;

    /**
     * The type of strings contained in a column, such as JSON or UUID.
     */
    private String subtype;

    private final SingleTargetReference targetReference;

    public TextColumnStatistics(int columnId) {
        this.targetReference = new SingleTargetReference(columnId);
    }

    @Override
    public SingleTargetReference getTargetReference() {
        return this.targetReference;
    }

    @Override
    public ConstraintSQLSerializer<TextColumnStatistics> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new SQLiteSerializer((SQLiteInterface) sqlInterface);
        }
        throw new RuntimeException("No serializer available for " + sqlInterface);
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public String getShortestValue() {
        return shortestValue;
    }

    public void setShortestValue(String shortestValue) {
        this.shortestValue = shortestValue;
    }

    public String getLongestValue() {
        return longestValue;
    }

    public void setLongestValue(String longestValue) {
        this.longestValue = longestValue;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

    /**
     * SQLite serializer for {@link TextColumnStatistics}.
     */
    public static class SQLiteSerializer implements ConstraintSQLSerializer<TextColumnStatistics> {

        /**
         * Name of the SQL table to store the {@link TextColumnStatistics} instances.
         */
        private static final String TABLE_NAME = "textColumnStatistics";

        private static final PreparedStatementBatchWriter.Factory<Object[]> INSERT_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + TABLE_NAME + " " +
                                "(constraintCollectionId, columnId, minValue, maxValue, shortestValue, longestValue, subtype) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (parameters, preparedStatement) -> {
                            preparedStatement.setInt(1, (Integer) parameters[0]);
                            preparedStatement.setInt(2, (Integer) parameters[1]);
                            preparedStatement.setString(3, (String) parameters[2]);
                            preparedStatement.setString(4, (String) parameters[3]);
                            preparedStatement.setString(5, (String) parameters[4]);
                            preparedStatement.setString(6, (String) parameters[5]);
                            preparedStatement.setString(7, (String) parameters[6]);
                        },
                        TABLE_NAME);

        private static final StrategyBasedPreparedQuery.Factory<Void> LOAD_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT columnId, minValue, maxValue, shortestValue, longestValue, subtype " +
                                "FROM %1$s;", TABLE_NAME),
                        PreparedStatementAdapter.VOID_ADAPTER,
                        TABLE_NAME);

        private static final StrategyBasedPreparedQuery.Factory<Integer> LOAD_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT columnId, minValue, maxValue, shortestValue, longestValue, subtype " +
                                "FROM %1$s " +
                                "WHERE constraintCollectionId = ?;", TABLE_NAME),
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        TABLE_NAME);


        private final SQLiteInterface sqLiteInterface;

        private final PreparedStatementBatchWriter<Object[]> insertWriter;

        private final DatabaseQuery<Void> loadQuery;

        private final DatabaseQuery<Integer> loadConstraintCollectionQuery;

        public SQLiteSerializer(SQLiteInterface sqLiteInterface) {
            this.sqLiteInterface = sqLiteInterface;
            try {
                final DatabaseAccess dba = this.sqLiteInterface.getDatabaseAccess();
                this.insertWriter = dba.createBatchWriter(INSERT_WRITER_FACTORY);
                this.loadQuery = dba.createQuery(LOAD_QUERY_FACTORY);
                this.loadConstraintCollectionQuery = dba.createQuery(LOAD_CONSTRAINTCOLLECTION_QUERY_FACTORY);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(TABLE_NAME);
        }

        @Override
        public void initializeTables() {
            if (!this.sqLiteInterface.tableExists(TABLE_NAME)) {
                String createTable = "CREATE TABLE [" + TABLE_NAME + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [constraintCollectionId] integer NOT NULL,\n" +
                        "    [columnId] integer NOT NULL,\n" +
                        "    [minValue] text,\n" +
                        "    [maxValue] text,\n" +
                        "    [shortestValue] text,\n" +
                        "    [longestValue] text,\n" +
                        "    [subtype] text,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "    FOREIGN KEY ([constraintCollectionId])\n" +
                        "    REFERENCES [ConstraintCollection] ([id]),\n" +
                        "    FOREIGN KEY ([columnId])\n" +
                        "    REFERENCES [Columnn] ([id])\n" +
                        ");";
                this.sqLiteInterface.executeCreateTableStatement(createTable);
            }
            if (!sqLiteInterface.tableExists(TABLE_NAME)) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void serialize(Constraint constraint, ConstraintCollection constraintCollection) {
            if (!(constraint instanceof TextColumnStatistics)) {
                throw new IllegalArgumentException();
            }
            TextColumnStatistics columnStatistics = (TextColumnStatistics) constraint;
            try {
                this.insertWriter.write(new Object[]{
                        constraintCollection.getId(),
                        columnStatistics.getTargetReference().getTargetId(),
                        columnStatistics.getMaxValue(),
                        columnStatistics.getMaxValue(),
                        columnStatistics.getShortestValue(),
                        columnStatistics.getLongestValue(),
                        columnStatistics.getSubtype()
                });
            } catch (SQLException e) {
                throw new RuntimeException("Serialization failed.", e);
            }
        }

        @Override
        public Collection<TextColumnStatistics> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection constraintCollection) {

            Collection<TextColumnStatistics> constraints = new LinkedList<>();
            try (ResultSet resultSet = constraintCollection == null ?
                    this.loadQuery.execute(null) :
                    this.loadConstraintCollectionQuery.execute(constraintCollection.getId())) {

                while (resultSet.next()) {
                    final TextColumnStatistics constraint = new TextColumnStatistics(resultSet.getInt("columnId"));
                    constraint.setMinValue(resultSet.getString("minValue"));
                    constraint.setMaxValue(resultSet.getString("maxValue"));
                    constraint.setShortestValue(resultSet.getString("shortestValue"));
                    constraint.setLongestValue(resultSet.getString("longestValue"));
                    constraint.setSubtype(resultSet.getString("subtype"));
                    constraints.add(constraint);
                }

            } catch (SQLException e) {
                throw new RuntimeException("Could not load constraint collection.", e);
            }
            return constraints;
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection constraintCollection) {
            // todo
            throw new RuntimeException("Not implemented.");
        }
    }
}
