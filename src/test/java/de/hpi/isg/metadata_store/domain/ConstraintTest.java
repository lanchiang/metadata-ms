package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.common.Observer;
import de.hpi.isg.metadata_store.domain.constraints.impl.DistinctValueCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TupleCount;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint;
import de.hpi.isg.metadata_store.domain.constraints.impl.TypeConstraint.TYPES;
import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.impl.SingleTargetReference;
import de.hpi.isg.metadata_store.domain.location.impl.DefaultLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class ConstraintTest {

    @SuppressWarnings("unchecked")
    @Test(expected = UnsupportedOperationException.class)
    public void testAddingConstraintsToUnmodifiableConstraintCollectionFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store1, mock(Table.class), "dummyColumn1",
                mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);
        
        ConstraintCollection constraintCollection = store1.createConstraintCollection();
        ((Collection<Constraint>)constraintCollection.getConstraints()).add(dummyTypeContraint);
    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnSchemasFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Schema dummySchema = DefaultSchema.buildAndRegister(store1, "dummySchema", mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummySchema),
                mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Ignore("for this test to pass, the DefaultMetadataStore must implement meaningful IDs")
    @Test(expected = IllegalArgumentException.class)
    public void testCreatingATypeConstraintOnTablesFails() {

        final MetadataStore store1 = new DefaultMetadataStore();

        final Table dummyTable = DefaultTable.buildAndRegister(store1, mock(Schema.class), "dummySchema",
                mock(Location.class));

        TypeConstraint.buildAndAddToCollection(new SingleTargetReference(dummyTable),
                mock(ConstraintCollection.class),
                TYPES.STRING);

    }

    @Test
    public void testTypeConstraint() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class),
                "dummyColumn1", mock(Location.class));

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final Constraint dummyTypeContraint1 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn), cC, TYPES.STRING);
        final Constraint dummyTypeContraint2 = TypeConstraint.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn), cC, TYPES.STRING);

        assertEquals(dummyTypeContraint1, dummyTypeContraint2);
    }

    @Test
    public void testTupleCount() {

        final Table dummyTable = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class),
                "dummyTable", new DefaultLocation());

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final TupleCount tupleCount1 = TupleCount.buildAndAddToCollection(
                new SingleTargetReference(dummyTable), cC, 1);
        final TupleCount tupleCount2 = TupleCount.build(
                new SingleTargetReference(dummyTable), cC, 1);

        assertEquals(tupleCount1.getNumTuples(), 1);

        assertEquals(tupleCount1, tupleCount2);
    }

    @Test
    public void testDistinctValueCount() {

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class),
                "dummyColumn1", mock(Location.class));

        final ConstraintCollection cC = mock(ConstraintCollection.class);
        final DistinctValueCount distinctValueCount1 = DistinctValueCount.buildAndAddToCollection(
                new SingleTargetReference(dummyColumn), cC, 1);
        final DistinctValueCount distinctValueCount2 = DistinctValueCount.build(
                new SingleTargetReference(dummyColumn), cC, 1);

        assertEquals(distinctValueCount2.getNumDistinctValues(), 1);

        assertEquals(distinctValueCount1, distinctValueCount2);
    }

    @Test
    public void testTypeConstraintOnAddedColumn() {

        final MetadataStore store = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(store, mock(Table.class), "dummyColumn3 ",
                mock(Location.class));

        store.getSchemas().add(
                DefaultSchema.buildAndRegister(store, "dummySchema", null).addTable(
                        DefaultTable.buildAndRegister(store, mock(Schema.class), "dummyTable", null).addColumn(
                                dummyColumn)));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store.createConstraintCollection();
        constraintCollection.add(dummyTypeContraint);
    }

    @Test(expected = NotAllTargetsInStoreException.class)
    public void testTypeConstraintOnNotAddedColumnFails() {

        final MetadataStore store2 = new DefaultMetadataStore();

        final Column dummyColumn = DefaultColumn.buildAndRegister(mock(Observer.class), mock(Table.class),
                "dummyColumn2", mock(Location.class));

        final Constraint dummyTypeContraint = TypeConstraint.buildAndAddToCollection(new SingleTargetReference(
                dummyColumn),
                mock(ConstraintCollection.class), TYPES.STRING);

        ConstraintCollection constraintCollection = store2.createConstraintCollection();
        constraintCollection.add(dummyTypeContraint);
    }

}
