/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.domain.constraints;

import de.hpi.isg.mdms.db.PreparedStatementAdapter;
import de.hpi.isg.mdms.db.query.DatabaseQuery;
import de.hpi.isg.mdms.db.query.StrategyBasedPreparedQuery;
import de.hpi.isg.mdms.db.write.DatabaseWriter;
import de.hpi.isg.mdms.db.write.PreparedStatementBatchWriter;
import de.hpi.isg.mdms.model.common.AbstractHashCodeAndEquals;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.TargetReference;
import de.hpi.isg.mdms.rdbms.ConstraintSQLSerializer;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.commons.lang3.Validate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Constraint implementation for an n-ary inclusion dependency.
 *
 * @author Sebastian Kruse
 */
public class InclusionDependency extends AbstractHashCodeAndEquals implements RDBMSConstraint {

    private static final Pattern UIND_PATTERN = Pattern.compile("\\[(\\d+),(\\d+)\\]");

    public static class InclusionDependencySQLiteSerializer implements ConstraintSQLSerializer<InclusionDependency> {

        private final static String tableName = "IND";
        private final static String referenceTableName = "INDPart";

        private final SQLInterface sqlInterface;

        DatabaseWriter<int[]> insertInclusionDependencyWriter;

        DatabaseWriter<Integer> deleteInclusionDependencyWriter;

        DatabaseWriter<int[]> insertINDPartWriter;

        DatabaseWriter<Integer> deleteINDPartWriter;

        DatabaseQuery<Void> queryInclusionDependencies;

        DatabaseQuery<Integer> queryInclusionDependenciesForConstraintCollection;

        //        DatabaseQuery<Integer> queryINDPart;
        private int currentConstraintIdMax = -1;

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + tableName + " (constraintId, constraintCollectionId) VALUES (?, ?);",
                        (parameters, preparedStatement) -> {
                            preparedStatement.setInt(1, parameters[0]);
                            preparedStatement.setInt(2, parameters[1]);
                        },
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_INCLUSIONDEPENDENCY_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + tableName + " where constraintId=?;",
                        (parameter, preparedStatement) -> preparedStatement.setInt(1, parameter),
                        tableName);

        private static final PreparedStatementBatchWriter.Factory<int[]> INSERT_INDPART_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "INSERT INTO " + referenceTableName
                                + " (constraintId, lhs, rhs) VALUES (?, ?, ?);",
                        (parameters, preparedStatement) -> {
                            preparedStatement.setInt(1, parameters[0]);
                            preparedStatement.setInt(2, parameters[1]);
                            preparedStatement.setInt(3, parameters[2]);
                        },
                        referenceTableName);

        private static final PreparedStatementBatchWriter.Factory<Integer> DELETE_INDPART_WRITER_FACTORY =
                new PreparedStatementBatchWriter.Factory<>(
                        "DELETE from " + referenceTableName
                                + " where constraintId=?;",
                        (parameter, preparedStatement) -> preparedStatement.setInt(1, parameter),
                        referenceTableName);

        private static final StrategyBasedPreparedQuery.Factory<Void> INCLUSIONDEPENDENCY_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT %1$s.constraintId as id, " +
                                "%1$s.constraintCollectionId as constraintCollectionId, " +
                                "group_concat('['||lhs||','||rhs||']', ',') as uinds " +
                                "FROM %1$s, %2$s " +
                                "WHERE %1$s.constraintId = %2$s.constraintId " +
                                "GROUP BY %1$s.constraintId, %1$s.constraintCollectionId;", tableName, referenceTableName),
                        PreparedStatementAdapter.VOID_ADAPTER,
                        tableName);

        private static final StrategyBasedPreparedQuery.Factory<Integer> INCLUSIONDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY =
                new StrategyBasedPreparedQuery.Factory<>(
                        String.format("SELECT %1$s.constraintId as id, " +
                                "%1$s.constraintCollectionId as constraintCollectionId, " +
                                "group_concat('['||lhs||','||rhs||']', ',') as uinds " +
                                "FROM %1$s, %2$s " +
                                "WHERE %1$s.constraintId = %2$s.constraintId " +
                                "AND %1$s.constraintCollectionId = ?" +
                                "GROUP BY %1$s.constraintId, %1$s.constraintCollectionId;", tableName, referenceTableName),
                        PreparedStatementAdapter.SINGLE_INT_ADAPTER,
                        tableName);

        public InclusionDependencySQLiteSerializer(SQLInterface sqlInterface) {
            this.sqlInterface = sqlInterface;

            try {
                this.deleteInclusionDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_INCLUSIONDEPENDENCY_WRITER_FACTORY);

                this.insertInclusionDependencyWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_INCLUSIONDEPENDENCY_WRITER_FACTORY);

                this.deleteINDPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        DELETE_INDPART_WRITER_FACTORY);

                this.insertINDPartWriter = sqlInterface.getDatabaseAccess().createBatchWriter(
                        INSERT_INDPART_WRITER_FACTORY);

                this.queryInclusionDependencies = sqlInterface.getDatabaseAccess().createQuery(
                        INCLUSIONDEPENDENCY_QUERY_FACTORY);

                this.queryInclusionDependenciesForConstraintCollection = sqlInterface.getDatabaseAccess()
                        .createQuery(
                                INCLUSIONDEPENDENCY_FOR_CONSTRAINTCOLLECTION_QUERY_FACTORY);

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void serialize(Constraint inclusionDependency, ConstraintCollection<? extends Constraint> constraintCollection) {
            ensureCurrentConstraintIdMaxInitialized();

            // for auto-increment id
            Integer constraintId = ++currentConstraintIdMax;

            Validate.isTrue(inclusionDependency instanceof InclusionDependency);
            try {
                insertInclusionDependencyWriter.write(new int[]{constraintId, constraintCollection.getId()});

                for (int i = 0; i < ((InclusionDependency) inclusionDependency).getArity(); i++) {
                    insertINDPartWriter.write(new int[]{constraintId,
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getDependentColumns()[i],
                            ((InclusionDependency) inclusionDependency).getTargetReference()
                                    .getReferencedColumns()[i]});
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public Collection<InclusionDependency> deserializeConstraintsOfConstraintCollection(
                ConstraintCollection<? extends Constraint> constraintCollection) {
            boolean retrieveConstraintCollection = constraintCollection == null;

            Collection<InclusionDependency> inclusionDependencies = new HashSet<>();

            try {
                ResultSet rsInclusionDependencies = retrieveConstraintCollection ?
                        queryInclusionDependencies.execute(null) : queryInclusionDependenciesForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsInclusionDependencies.next()) {
                    if (retrieveConstraintCollection) {
                        constraintCollection = (RDBMSConstraintCollection) this.sqlInterface
                                .getConstraintCollectionById(rsInclusionDependencies
                                        .getInt("constraintCollectionId"));
                    }
                    final Reference reference = parseInclusionDependencyReferences(rsInclusionDependencies.getString("uinds"));
                    inclusionDependencies.add(new InclusionDependency(reference));

                }
                rsInclusionDependencies.close();

                return inclusionDependencies;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private Reference parseInclusionDependencyReferences(String code) {
            final Matcher matcher = UIND_PATTERN.matcher(code);
            List<int[]> uinds = new ArrayList<>(5);
            while (matcher.find()) {
                uinds.add(new int[]{
                        Integer.parseInt(matcher.group(1)),
                        Integer.parseInt(matcher.group(2))
                });
            }
            int[] dep = new int[uinds.size()];
            int[] ref = new int[uinds.size()];
            for (int i = 0; i < uinds.size(); i++) {
                dep[i] = uinds.get(i)[0];
                ref[i] = uinds.get(i)[1];
            }
            return InclusionDependency.Reference.sortAndBuild(dep, ref);
        }

        @Override
        public List<String> getTableNames() {
            return Arrays.asList(tableName, referenceTableName);
        }

        @Override
        public void initializeTables() {
            if (!sqlInterface.tableExists(tableName)) {
                String createINDTable = "CREATE TABLE [" + tableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "	 [constraintCollectionId] integer NOT NULL,\n" +
                        "    PRIMARY KEY ([constraintId]),\n" +
                        "	 FOREIGN KEY ([constraintCollectionId])" +
                        "    REFERENCES [ConstraintCollection] ([id])" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createINDTable);
            }
            if (!sqlInterface.tableExists(referenceTableName)) {
                String createINDpartTable = "CREATE TABLE [" + referenceTableName + "]\n" +
                        "(\n" +
                        "    [constraintId] integer NOT NULL,\n" +
                        "    [lhs] integer NOT NULL,\n" +
                        "    [rhs] integer NOT NULL,\n" +
                        "    FOREIGN KEY ([lhs])\n" +
                        "    REFERENCES [Columnn] ([id]),\n" +
                        "    FOREIGN KEY ([constraintId])\n" +
                        "    REFERENCES [" + tableName + "] ([constraintId]),\n" +
                        "    FOREIGN KEY ([rhs])\n" +
                        "    REFERENCES [Columnn] ([id])\n" +
                        ");";
                this.sqlInterface.executeCreateTableStatement(createINDpartTable);
            }
            // check again and set allTablesExistChecked to true
            if (!(sqlInterface.tableExists(tableName) && sqlInterface.tableExists(referenceTableName))) {
                throw new IllegalStateException("Not all tables necessary for serializer were created.");
            }
        }

        @Override
        public void removeConstraintsOfConstraintCollection(ConstraintCollection<? extends Constraint> constraintCollection) {
            try {
                ResultSet rsINDs = queryInclusionDependenciesForConstraintCollection
                        .execute(constraintCollection.getId());
                while (rsINDs.next()) {
                    this.deleteInclusionDependencyWriter.write(rsINDs.getInt("id"));
                    this.deleteINDPartWriter.write(rsINDs.getInt("id"));
                }
                rsINDs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }


        private void ensureCurrentConstraintIdMaxInitialized() {
            if (this.currentConstraintIdMax != -1) {
                return;
            }

            try {
                this.currentConstraintIdMax = 0;
                try (ResultSet res = this.sqlInterface.getDatabaseAccess().query("SELECT MAX(constraintId) from " + getTableNames().get(0) + ";", getTableNames().get(0))) {
                    while (res.next()) {
                        if (this.currentConstraintIdMax < res.getInt("max(constraintId)")) {
                            this.currentConstraintIdMax = res.getInt("max(constraintId)");
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class Reference extends AbstractHashCodeAndEquals implements TargetReference {

        private static final long serialVersionUID = -861294530676768362L;

        private static int[] toIntArray(Column[] columns) {
            int[] intArray = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                intArray[i] = columns[i].getId();
            }
            return intArray;
        }

        /**
         * Creates an IND reference and in doing so puts the column pairs in the right order.
         *
         * @param dependentColumns  are the dependent columns in the IND
         * @param referencedColumns are the referneced columns in the IND
         * @return the canonical IND reference
         */
        public static Reference sortAndBuild(final Column[] dependentColumns, final Column[] referencedColumns) {
            return sortAndBuild(toIntArray(dependentColumns), toIntArray(referencedColumns));
        }

        /**
         * Creates an IND reference and in doing so puts the column pairs in the right order.
         *
         * @param dep are the dependent column IDs in the IND
         * @param ref are the referneced column IDs in the IND
         * @return the canonical IND reference
         */
        public static Reference sortAndBuild(int[] dep, int[] ref) {
            for (int j = ref.length - 1; j > 0; j--) {
                for (int i = j - 1; i >= 0; i--) {
                    if (dep[i] > dep[j] || (dep[i] == dep[j] && ref[i] > ref[j])) {
                        swap(dep, i, j);
                        swap(ref, i, j);
                    }
                }
            }
            return new Reference(dep, ref);
        }

        private static void swap(int[] array, int i, int j) {
            int temp = array[i];
            array[i] = array[j];
            array[j] = temp;
        }

        int[] dependentColumns;
        int[] referencedColumns;

        public Reference(final Column[] dependentColumns, final Column[] referencedColumns) {
            this(toIntArray(dependentColumns), toIntArray(referencedColumns));
        }

        public Reference(final int[] dependentColumnIds, final int[] referencedColumnIds) {
            this.dependentColumns = dependentColumnIds;
            this.referencedColumns = referencedColumnIds;
        }

        /**
         * Tests if this is a valid reference.
         *
         * @return whether it is a valid reference
         */
        public boolean isValid() {
            // Referenced and dependent side existing and similar?
            if (this.dependentColumns == null || this.referencedColumns == null || this.dependentColumns.length == 0
                    || this.dependentColumns.length != this.referencedColumns.length) {
                return false;
            }
            // Is the ordering of the IDs fulfilled?
            for (int i = 1; i < this.dependentColumns.length; i++) {
                if (this.dependentColumns[i - 1] < this.dependentColumns[i]) continue;
                if (this.dependentColumns[i - 1] > this.dependentColumns[i]) {
//            throw new IllegalArgumentException("Dependent column IDs are not in ascending order: " + Arrays.toString(dependentColumnIds));
                    return false;
                } else if (this.referencedColumns[i - 1] >= this.referencedColumns[i]) {
//            throw new IllegalArgumentException("Referenced column IDs are not in ascending order on equal dependent columns: " +
//                Arrays.toString(dependentColumnIds) + " < " + Arrays.toString(referencedColumnIds));
                    return false;
                }
            }

            return true;
        }


        @Override
        public IntCollection getAllTargetIds() {
            IntList allTargetIds = new IntArrayList(this.dependentColumns.length + this.referencedColumns.length);
            allTargetIds.addElements(0, this.dependentColumns);
            allTargetIds.addElements(allTargetIds.size(), this.referencedColumns);
            return allTargetIds;
        }

        /**
         * @return the dependentColumns
         */
        public int[] getDependentColumns() {
            return this.dependentColumns;
        }

        /**
         * @return the referencedColumns
         */
        public int[] getReferencedColumns() {
            return this.referencedColumns;
        }

        @Override
        public String toString() {
            return "Reference [dependentColumns=" + Arrays.toString(dependentColumns) + ", referencedColumns="
                    + Arrays.toString(referencedColumns) + "]";
        }
    }

    private static final long serialVersionUID = -932394088609862495L;
    private InclusionDependency.Reference target;

    @Deprecated
    public static InclusionDependency build(final InclusionDependency.Reference target) {
        InclusionDependency inclusionDependency = new InclusionDependency(target);
        return inclusionDependency;
    }

    public static InclusionDependency buildAndAddToCollection(final InclusionDependency.Reference target,
                                                              ConstraintCollection<InclusionDependency> constraintCollection) {
        InclusionDependency inclusionDependency = new InclusionDependency(target);
        constraintCollection.add(inclusionDependency);
        return inclusionDependency;
    }

    public InclusionDependency(final InclusionDependency.Reference target) {
        if (target.dependentColumns.length != target.referencedColumns.length) {
            throw new IllegalArgumentException("Number of dependent columns must equal number of referenced columns!");
        }
        this.target = target;
    }

    @Override
    public InclusionDependency.Reference getTargetReference() {
        return target;
    }

    @Override
    public String toString() {
        return "InclusionDependency [target=" + target + "]";
    }

    public int getArity() {
        return this.getTargetReference().getDependentColumns().length;
    }

    /**
     * Checks whether this inclusion dependency is implied by another inclusion dependency.
     *
     * @param that is the allegedly implying IND
     * @return whether this IND is implied
     */
    public boolean isImpliedBy(InclusionDependency that) {
        if (this.getArity() > that.getArity()) {
            return false;
        }

        // Co-iterate the two INDs and make use of the sorting of the column IDs.
        int thisI = 0, thatI = 0;
        while (thisI < this.getArity() && thatI < that.getArity() && (this.getArity() - thisI <= that.getArity() - thatI)) {
            int thisCol = this.getTargetReference().dependentColumns[thisI];
            int thatCol = that.getTargetReference().dependentColumns[thatI];
            if (thisCol == thatCol) {
                thisCol = this.getTargetReference().referencedColumns[thisI];
                thatCol = that.getTargetReference().referencedColumns[thatI];
            }
            if (thisCol == thatCol) {
                thisI++;
                thatI++;
            } else if (thisCol > thatCol) {
                thatI++;
            } else {
                return false;
            }
        }

        return thisI == this.getArity();
    }

    /**
     * Tests this inclusion dependency for triviality, i.e., whether the dependent and referenced sides are equal.
     *
     * @return whether this is a trivial inclusion dependency
     */
    public boolean isTrivial() {
        return Arrays.equals(this.target.dependentColumns, this.target.referencedColumns);
    }

    @Override
    public ConstraintSQLSerializer<InclusionDependency> getConstraintSQLSerializer(SQLInterface sqlInterface) {
        if (sqlInterface instanceof SQLiteInterface) {
            return new InclusionDependencySQLiteSerializer(sqlInterface);
        } else {
            throw new IllegalArgumentException("No suitable serializer found for: " + sqlInterface);
        }
    }

}