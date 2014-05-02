package org.apache.pig.test;

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
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.rules.ColumnPruneHelper;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOrcStoragePruning {

    File logFile;
    PigServer pigServer;
    static File basedir = new File("TestOrcStorageColumnPrune");
    String inputFile = basedir.getPath() + "/data";
    String inputFile_hcat = basedir.getPath() + "/data_hcat";
    String outputOrc = basedir.getPath() + "/orcout";

    private Driver driver;
    private static final String TEST_WAREHOUSE_DIR = basedir.getAbsolutePath() + "/warehouse";
    private static final String PARTITIONED_TABLE = "junit_parted_basic";
    private static final Log LOG = LogFactory.getLog(TestOrcStoragePruning.class);

    @Before
    public void setUp() throws Exception{
        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);

        pigServer = new PigServer(ExecType.LOCAL);
        String[] input = new String[] {"1\t2\t3","4\t5\t6"};
        Util.createInputFile(pigServer.getPigContext(), new File(inputFile).getPath(), input);

        // setup HCatalog
        HiveConf hiveConf = new HiveConf(this.getClass());
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
        driver = new Driver(hiveConf);
        SessionState.start(hiveConf);
        createTable(PARTITIONED_TABLE, "a int, b string, c timestamp, d double", "bkt string",
                "orc TBLPROPERTIES (\"orc.compress\"=\"NONE\")");

        //add rows to hcat table
        String[] input_hcat = new String[] {"1\tstring1\t2014-05-01T01:00Z\t1.0","2\tstring2\t2014-05-01T02:00Z\t2.0"};
        Util.createInputFile(pigServer.getPigContext(), new File(inputFile_hcat).getPath(), input_hcat);

    }

    @After
    public void teardown() throws Exception {
        if(pigServer != null) {
            pigServer.shutdown();
        }
        Util.deleteDirectory(basedir);
        //drop table
        driver.run("drop table if exists " + PARTITIONED_TABLE);
    }

    @Test
    public void testColumnPruneSimple() throws Exception {

        pigServer.setBatchOn();
        pigServer.registerQuery("A = load '" + inputFile + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("store A into '" + outputOrc + "' using OrcStorage();");
        pigServer.executeBatch();

        pigServer.setBatchOn();
        pigServer.registerQuery("B = load '" + outputOrc + "' using OrcStorage();");
        pigServer.registerQuery("C = foreach B generate a1, a2;");
        pigServer.store("C", outputOrc+"1", "OrcStorage");
        pigServer.executeBatch();

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

    @Test
    public void testColumnPruneWithPartitionFilter() throws Exception {

        pigServer.registerQuery("A = load '" + inputFile_hcat + "' as (a:int, b:chararray, c:datetime, d:double);");
        pigServer.registerQuery("store A into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');");

        pigServer.registerQuery("A = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
        Schema schema = pigServer.dumpSchema("A");
        assertEquals("{a: int,b: chararray,c: datetime,d: double,bkt: chararray}", schema.toString());
        pigServer.store("A", outputOrc, "OrcStorage");

        pigServer.registerQuery("B = load '" + outputOrc + "' using OrcStorage();");
        pigServer.registerQuery("C = filter B by bkt == '0';");
        pigServer.registerQuery("D = foreach C generate a, b;");
        pigServer.store("D", outputOrc + "1", "OrcStorage");
        schema = pigServer.dumpSchema("D");
        assertEquals("{a: int,b: chararray}", schema.toString());
        // columns a,b ($0, $1) required and c,d ($2, $3) pruned
        assertTrue(Util.checkLogFileMessage(new String[]{"Columns pruned for B: $2, $3"}, logFile));
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
        + "STORE A INTO '" + inputFile+"1" + "' USING OrcStorage();";
        pigServer.registerQuery(query);

        query = "B = LOAD '" + inputFile+"1" + "' USING OrcStorage();"
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
                + "STORE A INTO '" + inputFile+"A" + "' USING OrcStorage();"
                + "STORE B INTO '" + inputFile+"B" + "' USING OrcStorage();";
        pigServer.registerQuery(query);

        //run query
        query = "C = LOAD '" + inputFile+"A" + "' USING OrcStorage();"
                + "D = LOAD '" + inputFile+"B" + "' USING OrcStorage();"
                + "C1 = FILTER C BY a1 > 1 AND a2 == 'alice';"
                + "D1 = FOREACH D GENERATE b1, a2, b3;"
                + "E = JOIN C1 BY a2, D1 BY a2;"
                + "STORE E INTO 'out' USING mock.Storage();";
        org.apache.pig.newplan.logical.relational.LogicalPlan plan = Util.buildLp(pigServer, query);
        plan = Util.optimizeNewLP(plan);
        assertEquals(2, plan.getSources().size());
        LOLoad load = (LOLoad) plan.getSources().get(0);
        Object annotation = load.getAnnotation(ColumnPruneHelper.REQUIREDCOLS);
        assertNotNull(annotation);
        // assert columns pruned

    }

    private void createTable(String tableName, String schema, String partition, String storageFormat) throws Exception {
        String cmd = "CREATE TABLE " + tableName + "(" + schema + ") PARTITIONED BY (" + partition + ") STORED AS "
                + storageFormat;
        LOG.debug("Executing: " + cmd);
        CommandProcessorResponse cpr = driver.run(cmd);
        if (cpr.getResponseCode() != 0) {
            throw new IOException("Failed to execute \"" + cmd + "\". Driver returned " + cpr.getResponseCode()
                    + " Error: " + cpr.getErrorMessage());
        }
    }
}
