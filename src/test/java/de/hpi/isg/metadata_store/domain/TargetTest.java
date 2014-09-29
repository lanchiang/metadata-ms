package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashSet;

import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.location.impl.HDFSLocation;
import de.hpi.isg.metadata_store.domain.location.impl.IndexedLocation;
import de.hpi.isg.metadata_store.domain.targets.Column;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.domain.targets.Table;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultColumn;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultSchema;
import de.hpi.isg.metadata_store.domain.targets.impl.DefaultTable;

public class TargetTest {

    @Test
    public void testColumnHashCodeAndEquals() {
	final MetadataStore store = new DefaultMetadataStore();

	final HDFSLocation loc = new HDFSLocation("foobar");

	final Column column1 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo", loc);

	final Column column3 = DefaultColumn.buildAndRegister(store, mock(Table.class), "foo2", loc);

	final HashSet<Target> set = new HashSet<Target>();
	set.add(column1);
	assertTrue(set.contains(column1));
	assertFalse(set.contains(column3));
    }

    @Test
    public void testSchemaEquals() {
	// setup schema
	final Schema dummySchema = DefaultSchema.buildAndRegister(mock(MetadataStore.class), 2, "PDB",
		new HDFSLocation("hdfs://foobar"));

	final HDFSLocation dummyTableLocation = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 3, "dummyTable", dummyTableLocation);

	DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation));

	// setup schema
	final Schema dummySchema2 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), 2, "PDB",
		new HDFSLocation("hdfs://foobar"));

	final HDFSLocation dummyTableLocation2 = new HDFSLocation("hdfs://foobar/dummyTable.csv");

	DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 3, "dummyTable", dummyTableLocation2);

	DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), 4, "dummyColumn", new IndexedLocation(0,
		dummyTableLocation2));

	assertEquals(dummySchema, dummySchema2);

    }

    @Test
    public void testSchemaHashCodeAndEquals() {
	final HDFSLocation loc = new HDFSLocation("foobar");

	final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class), mock(Table.class), "foo", loc);

	final Table table1 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), "foo", loc).addColumn(column1);

	final Schema schema1 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc).addTable(table1);
	final Schema schema2 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo", loc).addTable(table1);

	final Schema schema3 = DefaultSchema.buildAndRegister(mock(MetadataStore.class), "foo2", loc);

	assertEquals(schema1, schema2);

	assertEquals(schema1.hashCode(), schema2.hashCode());
	assertFalse(schema1.equals(schema3));
    }

    @Test
    public void testTableHashCodeAndEquals() {

	final HDFSLocation loc = new HDFSLocation("foobar");

	final Column column1 = DefaultColumn.buildAndRegister(mock(MetadataStore.class),mock(Table.class), 1, "foo", loc);

	final Table table1 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class),  2, "foo", loc).addColumn(column1);

	final Table table2 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 2, "foo", loc).addColumn(column1);

	final Table table3 = DefaultTable.buildAndRegister(mock(MetadataStore.class), mock(Schema.class), 3, "foo2", loc);

	assertEquals(table1, table2);

	assertEquals(table1.hashCode(), table2.hashCode());
	assertFalse(table1.equals(table3));
    }
}
