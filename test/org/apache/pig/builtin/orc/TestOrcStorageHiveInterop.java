package org.apache.pig.builtin.orc;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOrcStorageHiveInterop {

    PigServer pigServer;
    FileSystem filesystem;
    static File basedir = new File("TestOrcStorageHiveInterop");
    private static final String ORC_OUT = basedir.getPath() + "/orcout";
    private static final String OUTPUT = basedir.getPath() + "/pigout";
    Reader orcReader;

    private Driver driver;
    private static final String TEST_WAREHOUSE_DIR = basedir.getAbsolutePath() + "/warehouse";
    private static final String PARTITIONED_TABLE = "partitioned_table";
    private static final String NON_PART_TABLE = "non_partitioned_table";

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
        filesystem = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        pigServer.mkdirs(basedir.getAbsolutePath());
        setupHCatalog();
        // partitioned_table
    }

    @After
    public void teardown() throws Exception {
        if(pigServer != null) {
            pigServer.shutdown();
        }
        Util.deleteDirectory(basedir);
        dropTables();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOrcStorageHiveInterop() throws Exception {
        //bag of tuple
        createHiveTable(NON_PART_TABLE, "a ARRAY<STRUCT<i:INT>>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        ObjectInspector oi = executePig("a:bag{tuple(i:int)}", tuple(bag(tuple(100))));
        assertEquals(Category.LIST, oi.getCategory()); //output relation is always bag
        StructObjectInspector soi = (StructObjectInspector) ((ListObjectInspector)oi).getListElementObjectInspector();
        oi = soi.getAllStructFieldRefs().get(0).getFieldObjectInspector();
        assertEquals(Category.PRIMITIVE, oi.getCategory());
        Object row = orcReader.rows(null).next(null);
        List<OrcStruct> x = (List<OrcStruct>) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertEquals("{100}", x.get(0).toString());
        dropTables();
        Util.deleteDirectory(basedir);

        // bag of bag
        createHiveTable(NON_PART_TABLE, "a ARRAY<STRUCT<b:ARRAY<STRUCT<i:INT>>>>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        oi = executePig("a:bag{tuple(b:bag{tuple(i:int)})}", tuple(bag(tuple(bag(tuple(100))))));
        soi = (StructObjectInspector) ((ListObjectInspector)oi).getListElementObjectInspector();
        oi = soi.getAllStructFieldRefs().get(0).getFieldObjectInspector();
        assertEquals(Category.LIST, oi.getCategory()); //not primitive like preceding test
        soi = (StructObjectInspector) ((ListObjectInspector)oi).getListElementObjectInspector();
        row = orcReader.rows(null).next(null);
        x = (List<OrcStruct>) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertEquals("{[{100}]}", x.get(0).toString());
        dropTables();
        Util.deleteDirectory(basedir);

        // tuple of tuple
        createHiveTable(NON_PART_TABLE, "a STRUCT<i:INT>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        oi = executePig("a:tuple(i:int))", tuple(tuple(100)));
        assertEquals(Category.LIST, oi.getCategory());
        soi = (StructObjectInspector) ((ListObjectInspector)oi).getListElementObjectInspector();
        oi = soi.getAllStructFieldRefs().get(0).getFieldObjectInspector();
        assertEquals(Category.STRUCT, oi.getCategory()); //not primitive
        row = orcReader.rows(null).next(null);
        x = (List<OrcStruct>) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertEquals("(100)", x.get(0).toString());
        dropTables();
        Util.deleteDirectory(basedir);

        // tuple of map
        createHiveTable(NON_PART_TABLE, "a STRUCT<MAP<chararray, int>>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("alice", 123);
        oi = executePig("a:tuple(map[])", tuple(tuple(map1)));

        // bag of tuple of map
        createHiveTable(NON_PART_TABLE, "a ARRAY<STRUCT<m:MAP<chararray, int>>>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        oi = executePig("a:bag{tuple(m:map[])}", tuple(bag(tuple(map1))));

        // map of map
        createHiveTable(NON_PART_TABLE, "a MAP<m:MAP<chararray, int>>", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
        Map<Map<String, Object>, Object> map2 = new HashMap<Map<String, Object>, Object>();
        map2.put(map1, 456);
        oi = executePig("a:map[m:map[]]", tuple(map2));

        // TODO binary/bytearray

    }

    private void createHiveTable(String tableName, String schema, String partition, String storageFormat) throws Exception {
        String cmd;
        if (partition != null) {
            cmd = "CREATE TABLE " + tableName + "(" + schema + ") PARTITIONED BY (" + partition + ") STORED AS "
                    + storageFormat;
        }
        else {
            cmd = "CREATE TABLE " + tableName + "(" + schema + ") STORED AS "
                    + storageFormat;
        }
        CommandProcessorResponse cpr = driver.run(cmd);
        if (cpr.getResponseCode() != 0) {
            throw new IOException("Failed to execute \"" + cmd + "\". Driver returned " + cpr.getResponseCode()
                    + " Error: " + cpr.getErrorMessage());
        }
    }

    private void setupHCatalog() throws Exception {
        HiveConf hiveConf = new HiveConf(this.getClass());
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
        driver = new Driver(hiveConf);
        SessionState.start(hiveConf);
        /*createHiveTable(PARTITIONED_TABLE, "a string, b int, c timestamp, d double", "bkt string",
                "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");*/
    }

    private void dropTables() throws Exception {
        //drop tables
        driver.run("drop table if exists " + NON_PART_TABLE);
        //driver.run("drop table if exists " + PARTITIONED_TABLE);
    }

    private ObjectInspector executePig(String schema, Tuple tuple) throws Exception {
        Data data = resetData(pigServer);
        data.set("input", schema, tuple);
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (" + schema + ");");
        pigServer.store("A", NON_PART_TABLE, "org.apache.hive.hcatalog.pig.HCatStorer()");
        pigServer.registerQuery("B = load '" + NON_PART_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
        ExecJob job = pigServer.store("B", ORC_OUT, "OrcStorage");
        assertEquals(1, job.getStatistics().getNumberSuccessfulJobs());

        orcReader = OrcFile.createReader(filesystem, new Path(ORC_OUT, "part-m-00000"));
/*      FieldSchema fs = job.getPOStore().getSchema().getField(0);
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.BAG, rs.getType());
        assertEquals(ListTypeInfo.class, OrcUtils.getTypeInfo(rs).getClass()); // bag is a list of tuples
*/
        StructObjectInspector soi = (StructObjectInspector) orcReader.getObjectInspector();
        StructField bagObj = soi.getAllStructFieldRefs().get(0);
        return bagObj.getFieldObjectInspector();
    }

}
