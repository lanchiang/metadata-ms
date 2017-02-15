package de.hpi.isg.mdms.dependencies;

import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.exceptions.NotAllTargetsInStoreException;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class ConstraintTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, Mockito.mock(Table.class), "dummyColumn1", null,
                Mockito.mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), "VARCHAR");

        ConstraintCollection constraintCollection = store1.createConstraintCollection(null, TestConstraint.class);
        ((Collection<Constraint>) constraintCollection.getConstraints()).add(dummyTypeContraint);
    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnSchemasFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "dummySchema", null, Mockito.mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummySchema.getId()),
                Mockito.mock(ConstraintCollection.class),
                "VARCHAR");

    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnTablesFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, Mockito.mock(Schema.class), "dummySchema", null,
                Mockito.mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummyTable.getId()),
                Mockito.mock(ConstraintCollection.class),
                "VARCHAR");

    }

    @Test
    public void testTypeConstraint() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));

        final ConstraintCollection<TypeConstraint> cC = Mockito.mock(ConstraintCollection.class);
        final Constraint dummyTypeContraint1 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, "VARCHAR");
        final Constraint dummyTypeContraint2 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, "VARCHAR");

        Assert.assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test
    public void testTupleCount() {

        final Table dummyTable = DefaultTable.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Schema.class),
                "dummyTable", null, new DefaultLocation());

        final ConstraintCollection<TupleCount> cC = Mockito.mock(ConstraintCollection.class);
        final TupleCount tupleCount1 = TupleCount.buildAndAddToCollection(
                new SingleTargetReference(dummyTable.getId()), cC, 1);
        final TupleCount tupleCount2 = TupleCount.build(
                new SingleTargetReference(dummyTable.getId()), cC, 1);

        assertEquals(tupleCount1.getNumTuples(), 1);

        assertEquals(tupleCount1, tupleCount2);
    }

    @Test
    public void testDistinctValueCount() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));

        final ConstraintCollection<DistinctValueCount> cC = Mockito.mock(ConstraintCollection.class);
        final DistinctValueCount distinctValueCount1 = DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn.getId()), cC, 1);
        final DistinctValueCount distinctValueCount2 = new DistinctValueCount(
                new SingleTargetReference(dummyColumn.getId()), 1);

        assertEquals(distinctValueCount2.getNumDistinctValues(), 1);

        assertEquals(distinctValueCount1, distinctValueCount2);
    }

    @Test
    public void testInclusionDependency() {

      MetadataStore metadataStore = new DefaultMetadataStore();
      final Column dummyColumn1 = DefaultColumn.buildAndRegister(metadataStore, Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(metadataStore, Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final InclusionDependency ind1 = new InclusionDependency(InclusionDependency.Reference.sortAndBuild(new Column[]{
                dummyColumn1,
                dummyColumn2},
            new Column[]{dummyColumn2, dummyColumn1}));
        final InclusionDependency ind2 = new InclusionDependency(InclusionDependency.Reference.sortAndBuild(new Column[]{
                dummyColumn1,
                dummyColumn2},
            new Column[]{dummyColumn2, dummyColumn1}));

        assertEquals(ind1.getArity(), 2);

        assertEquals(ind1, ind2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssymetricInclusionDependencyFails() {

        final Column dummyColumn1 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));
        final Column dummyColumn3 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn3", null, Mockito.mock(Location.class));

        final ConstraintCollection<? extends Constraint> cC = Mockito.mock(ConstraintCollection.class);
        @SuppressWarnings("unused")
        final InclusionDependency ind1 = new InclusionDependency(new InclusionDependency.Reference(
                new Column[] { dummyColumn1 },
                new Column[] { dummyColumn2, dummyColumn3 }));
    }

    @Test
    public void testUniqueColumnCombination() {

        final Column dummyColumn1 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn1", null, Mockito.mock(Location.class));
        final Column dummyColumn2 = DefaultColumn.buildAndRegister(Mockito.mock(MetadataStore.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final ConstraintCollection<? extends Constraint> cC = Mockito.mock(ConstraintCollection.class);
        final UniqueColumnCombination ucc1 = UniqueColumnCombination.build(new UniqueColumnCombination.Reference(
                new int[] { dummyColumn1.getId(), dummyColumn2.getId() }), cC);
        final UniqueColumnCombination ucc2 = UniqueColumnCombination.build(new UniqueColumnCombination.Reference(
                new int[] { dummyColumn1.getId(), dummyColumn2.getId() }), cC);

        assertEquals(ucc1.getArity(), 2);

        assertEquals(ucc1, ucc2);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

        final MetadataStore store = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store, "dummySchema", null, null);
        store.getSchemas().add(dummySchema);
        final Column dummyColumn = dummySchema.addTable(store, "dummyTable", "", Mockito.mock(Location.class)).addColumn(store,
                "dummyColumn3 ", "", 1);

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), "VARCHAR");

        ConstraintCollection<Constraint> constraintCollection = store.createConstraintCollection(null, Constraint.class);
        constraintCollection.add(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

        final MetadataStore store2 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(Mockito.mock(Observer.class), Mockito.mock(Table.class),
                "dummyColumn2", null, Mockito.mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn.getId()),
                Mockito.mock(ConstraintCollection.class), "VARCHAR");

        ConstraintCollection<Constraint> constraintCollection = store2.createConstraintCollection(null, Constraint.class);
        constraintCollection.add(dummyTypeContraint);
    }

}
