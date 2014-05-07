package org.apache.pig.builtin.orc;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.rules.ColumnPruneHelper;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.OutputStats;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOrcStoragePruning {

    File logFile;
    PigServer pigServer;
    static File basedir = new File("TestOrcStorageColumnPrune");
    String input_hcat = basedir.getPath() + "/data_hcat";
    String orcfile = basedir.getPath() + "/orcout";

    private Driver driver;
    private static final String TEST_WAREHOUSE_DIR = basedir.getAbsolutePath() + "/warehouse";
    private static final String PARTITIONED_TABLE = "partitioned_table";
    private static final String SIMPLE_TABLE = "non_partitioned_table";
    private static final Log LOG = LogFactory.getLog(TestOrcStoragePruning.class);

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);

        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);
    }

    @After
    public void teardown() throws Exception {
        if(pigServer != null) {
            pigServer.shutdown();
        }
        Util.deleteDirectory(basedir);
    }

    @Test
    public void testColumnPruneSimple() throws Exception {

        Data data = resetData(pigServer);
        data.set("input", "a0:int, a1:int, a2:int", tuple(1,2,3), tuple(4,5,6));
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("store A into '" + orcfile + "' using OrcStorage();");

        pigServer.registerQuery("B = load '" + orcfile + "' using OrcStorage();");
        pigServer.registerQuery("C = foreach B generate a1, a2;");
        pigServer.store("C", orcfile+"1", "OrcStorage");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals(3, t.get(1));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals(5, t.get(0));
        assertEquals(6, t.get(1));

        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for B: $0"}, logFile));
    }

    private void setupHCatalog() throws Exception {

        // setup HCatalog
        HiveConf hiveConf = new HiveConf(this.getClass());
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
        driver = new Driver(hiveConf);
        SessionState.start(hiveConf);
        driver.run("drop table if exists " + SIMPLE_TABLE);
        driver.run("drop table if exists " + PARTITIONED_TABLE);
        createHiveTable(PARTITIONED_TABLE, "a string, b int, c timestamp, d double", "bkt string",
                "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");

        createHiveTable(SIMPLE_TABLE, "a int, b string", null, "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");
    }

    private void tearDownHCatalog() throws Exception {
        //drop tables
        driver.run("drop table if exists " + PARTITIONED_TABLE);
        driver.run("drop table if exists " + SIMPLE_TABLE);
    }

    @Test
    public void testColumnPruneWithPartitionFilter() throws Exception {

        setupHCatalog();

        Data data = resetData(pigServer);
        data.set("input", "a:chararray, b:int, c:datetime, d:double",
                tuple("string1", 1, new DateTime("2014-05-01T01:00Z"), 1.0),
                tuple("string2", 2, new DateTime("2014-05-01T02:00Z"), 2.0));

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a:chararray, b:int, c:datetime, d:double);");
        pigServer.registerQuery("store A into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');");

        pigServer.registerQuery("A = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
        Schema schema = pigServer.dumpSchema("A");
        assertEquals("{a: chararray,b: int,c: datetime,d: double,bkt: chararray}", schema.toString());
        pigServer.store("A", orcfile, "OrcStorage");

        pigServer.registerQuery("B = load '" + orcfile + "' using OrcStorage();");
        pigServer.registerQuery("C = filter B by bkt == '0';");
        pigServer.registerQuery("D = foreach C generate a, b;");
        pigServer.store("D", orcfile + "1", "OrcStorage");
        schema = pigServer.dumpSchema("D");
        assertEquals("{a: chararray,b: int}", schema.toString());
        // columns a,b ($0, $1) required and c,d ($2, $3) pruned
        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for B: $2, $3"}, logFile));

        tearDownHCatalog();

    }

    @Test
    public void testMapKeyPrune() throws Exception {

        Data data = resetData(pigServer);
        Map<String, String> mapv1 = new HashMap<String, String>();
        mapv1.put("name", "bob");
        mapv1.put("gender", "m");
        mapv1.put("empid", "val1");
        Map<String, String> mapv2 = new HashMap<String, String>();
        mapv2.put("name", "alice");
        mapv2.put("gender", "f");
        mapv2.put("empid", "val2");
        data.set("testPrune", "a:int, b:float, m:map[chararray]", tuple(1,(float)1.0,mapv1), tuple(2,(float)2.0,mapv2));

        String query = "A = LOAD 'testPrune' USING mock.Storage() as (a:int, b:float, m:map[]);"
        + "STORE A INTO '" + orcfile + "' USING OrcStorage();";
        pigServer.registerQuery(query);

        query = "B = LOAD '" + orcfile + "' USING OrcStorage();"
        + "C = filter B by m#'name' == 'alice';"
        + "D = FOREACH C generate a,b,m#'empid';"
        + "STORE D INTO 'out' USING mock.Storage();";
        org.apache.pig.newplan.logical.relational.LogicalPlan plan = Util.buildLp(pigServer, query);
        plan = Util.optimizeNewLP(plan);
        assertEquals(1, plan.getSources().size());
        LOLoad load = (LOLoad) plan.getSources().get(0);
        Map<Long, Set<String>> annotation = (Map<Long, Set<String>>) load
                .getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertNotNull(annotation);
        assertEquals(1, annotation.keySet().size());
        Set<String> keys = annotation.get(2); //column index $2 is map
        assertEquals(2, keys.size());
        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("empid"));
        // map keys [empid, name] required
        assertTrue(Util.checkLogFileMessage(new String[]{"Map key required for B: $2->[empid, name]"}, logFile));

        List<Tuple> out = data.get("testPrune");
        assertEquals(out.get(0), tuple(1,(float)1.0,mapv1));
    }

    @Test
    public void testColumnPruneTwoTables() throws Exception {

        //prepare two datasets
        Data data = resetData(pigServer);
        Map<String, Double> mapv1 = new HashMap<String, Double>();
        mapv1.put("empid", 123.456);
        Map<String, Double> mapv2 = new HashMap<String, Double>();
        mapv2.put("empid", 234.567);
        data.set("data-A", "a1:int, a2:chararray, a3:map[double], a4:int", tuple(1, "bob", mapv1, 10), tuple(2, "alice", mapv2, 20));
        data.set("data-B", "b1:chararray, a2:chararray, b3:int", tuple("home", "12", 34), tuple("work", "56", 78));
        String query = "A = LOAD 'data-A' USING mock.Storage() as (a1:int, a2:chararray, a3:map[], a4:int);"
                + "B = LOAD 'data-B' USING mock.Storage() as (b1:chararray, a2:chararray, b3:int);"
                + "STORE A INTO '" + orcfile+"A" + "' USING OrcStorage();"
                + "STORE B INTO '" + orcfile+"B" + "' USING OrcStorage();";
        pigServer.registerQuery(query);

        //run query
        query = "A = LOAD '" + orcfile+"A" + "' USING OrcStorage();"
                + "B = LOAD '" + orcfile+"B" + "' USING OrcStorage();"
                + "C = FILTER A BY a1 > 1 AND a2 == 'alice';"
                + "D = JOIN C BY a2, B BY a2;"
                + "E = FOREACH D GENERATE b1, C::a2, b3;"
                + "STORE E INTO 'out' USING mock.Storage();";
        pigServer.registerQuery(query);
        org.apache.pig.newplan.logical.relational.LogicalPlan plan = Util.buildLp(pigServer, query);
        plan = Util.optimizeNewLP(plan);
        assertEquals(2, plan.getSources().size());
        // Data B has nothing pruned
        LOLoad load = (LOLoad) plan.getSources().get(0);
        Object annotation = load.getAnnotation(ColumnPruneHelper.REQUIREDCOLS);
        assertNull(annotation);
        // Data A has columns pruned
        load = (LOLoad) plan.getSources().get(1);
        Set<Integer> cols = (Set<Integer>) load.getAnnotation(ColumnPruneHelper.REQUIREDCOLS);
        assertNotNull(cols);
        assertEquals(2, cols.size());
        System.out.println(cols);
        assertTrue(cols.contains(0));
        assertTrue(cols.contains(1));

        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for A: $2, $3"}, logFile));
    }

    @Test
    public void testHCatLoaderTwoTables() throws Exception {

        setupHCatalog();
        Data data = resetData(pigServer);
        data.set("testPrune", "a:chararray, b:int, c:datetime, d:double", tuple("classA", 11, new DateTime(), 3.5), tuple("classB", 22, new DateTime(), 4.0));
        pigServer.registerQuery("A = LOAD 'testPrune' USING mock.Storage() as (a:chararray, b:int, c:datetime, d:double);");
        pigServer.registerQuery("store A into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');");
        data = resetData(pigServer);
        data.set("testPrune", "a:int, b:chararray", tuple(11, "classA"), tuple(22, "classB"));
        pigServer.registerQuery("B = LOAD 'testPrune' USING mock.Storage() as (a:int, b:chararray);");
        pigServer.registerQuery("store B into '" + SIMPLE_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();");

        pigServer.setBatchOn();
        String query = "A = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();"
        + "B = load '" + SIMPLE_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();"
        + "A1 = foreach A generate a, b;"
        + "C = join A1 by b, B by a;"
        + "C1 = foreach C generate A1::a;"
        + "store C1 into 'out' using mock.Storage();";
        pigServer.registerQuery(query);
        ExecJob job = pigServer.executeBatch().get(0);

        // JIRA PIG-3927
        OutputStats outstats = job.getStatistics().getOutputStats().get(0);
        String expectedCols = "0,0,1";
        assertEquals(expectedCols, outstats.getConf().get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        // following method gives deduped cols to record reader
        List<Integer> readerCols = ColumnProjectionUtils.getReadColumnIDs(outstats.getConf());
        assertEquals(0, (int)readerCols.get(0));
        assertEquals(1, (int)readerCols.get(1));

        org.apache.pig.newplan.logical.relational.LogicalPlan plan = Util.buildLp(pigServer, query);
        plan = Util.optimizeNewLP(plan);
        assertEquals(2, plan.getSources().size());
        // Data A and B columns required are different
        LOLoad load = (LOLoad) plan.getSources().get(0);
        Set<Integer> annotation = (Set<Integer>) load.getAnnotation(ColumnPruneHelper.REQUIREDCOLS);
        assertEquals(1, annotation.size());
        assertTrue(annotation.contains(0));
        assertFalse(annotation.contains(1));
        load = (LOLoad) plan.getSources().get(1);
        annotation = (Set<Integer>) load.getAnnotation(ColumnPruneHelper.REQUIREDCOLS);
        assertEquals(2, annotation.size());
        assertTrue(annotation.contains(0));
        assertTrue(annotation.contains(1));

        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for A: $2, $3, $4"}, logFile));
        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for B: $1"}, logFile));

        tearDownHCatalog();
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
        LOG.debug("Executing: " + cmd);
        CommandProcessorResponse cpr = driver.run(cmd);
        if (cpr.getResponseCode() != 0) {
            throw new IOException("Failed to execute \"" + cmd + "\". Driver returned " + cpr.getResponseCode()
                    + " Error: " + cpr.getErrorMessage());
        }
    }
}
