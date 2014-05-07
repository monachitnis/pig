package org.apache.pig.builtin.orc;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.newplan.FilterExtractor;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases to verify Pig's OrcStorage respects predicate pushdown and resulting Orc records are accurate
 * TODO verify against predicate strings from pig script once API implemented,
 * instead of test conditions directly passed to SearchArgument
 * @throws Exception
 */
public class TestOrcStoragePredicatePushdown {

    PigServer pigServer;
    static File basedir = new File("TestOrcStoragePredicatePushdown");
    String inputFile = basedir.getPath() + "/data";

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @After
    public void teardown() throws Exception {
        if (pigServer != null) {
            pigServer.shutdown();
        }
        Util.deleteDirectory(basedir);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPredicatePushdown() throws Exception {
        Data data = resetData(pigServer);
        Tuple[] tups = new Tuple[3000];
        for (int i = 0; i < tups.length; i++) {
            tups[i] = TupleFactory.getInstance().newTuple(
                    Arrays.asList(i + 1000, (float) i + 1000, "string" + (i + 1000)));
        }
        data.set("input", "a:int, b:float, c:chararray", tups);

        // script having column predicates
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a:int, b:float, c:chararray);");
        pigServer.store("A",inputFile,"OrcStorage('-r 1000')");

        Configuration conf = ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties());
        FileSystem fs = FileSystem.get(conf);
        Reader reader = OrcFile.createReader(new Path(inputFile, "part-m-00000"), OrcFile.readerOptions(conf)
                .filesystem(fs));
        assertEquals(3000, reader.getNumberOfRows());
        assertEquals(1000, reader.getRowIndexStride());


        SearchArgument sarg = SearchArgument.FACTORY.newBuilder().startAnd().startNot().lessThan("a", 2000).end()
                .lessThan("a", 3000).end().build();
        RecordReader rows = reader.rowsOptions(new Reader.Options().range(0L, Long.MAX_VALUE)
                .include(new boolean[]{true, true, true, true}).searchArgument(sarg, new String[]{null, "a", "b", "c"}));
        assertEquals(1000L, rows.getRowNumber());

        // look through the file with no rows selected
        sarg = SearchArgument.FACTORY.newBuilder().startAnd().lessThan("a", 1000).end().build();
        rows = reader.rowsOptions(new Reader.Options().range(0L, Long.MAX_VALUE)
                .include(new boolean[]{true, true, true, true}).searchArgument(sarg, new String[]{null, "a", "b", "c"}));
        assertEquals(3000L, rows.getRowNumber());
        assertTrue(!rows.hasNext());

        // select first 1000 and last 1000 rows
        sarg = SearchArgument.FACTORY.newBuilder().startOr().lessThan("a", 1500).startNot()
                .lessThan("a", 3500).end().end().build();
        rows = reader.rowsOptions(new Reader.Options().range(0L, Long.MAX_VALUE)
                .include(new boolean[]{true, true, true, true}).searchArgument(sarg, new String[]{null, "a", "b", "c"}));
        OrcStruct row = null;
        for (int i = 0; i < 1000; ++i) {
            assertTrue(rows.hasNext());
            assertEquals(i, rows.getRowNumber());
            row = (OrcStruct) rows.next(row);
            assertEquals("{"+(1000+i)+", "+(float)(1000+i)+", "+"string"+(1000+i)+"}", row.toString());
        }
        for (int i = 3000; i < 4000; ++i) {
            assertTrue(rows.hasNext());
            assertEquals(i, rows.getRowNumber()+1000); //skipping over 1000 rows
            row = (OrcStruct) rows.next(row);
            assertEquals("{"+i+", "+(float)i+", "+"string"+i+"}", row.toString());

        }
        assertTrue(!rows.hasNext());
        assertEquals(3000L, rows.getRowNumber()); // skips covered

        String query = "A = load '" + inputFile + "' using OrcStorage();"
                + "B = filter A by b > 3.5 AND a < 6;" // filtering to get 2 output tuples
                + "C = foreach B generate a;"
                + "store C into 'output' using mock.Storage();";

        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOForEach fe = (LOForEach) newLogicalPlan.getPredecessors(op).get(0);
        LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(fe).get(0);
        FilterExtractor extractor = new FilterExtractor(
                filter.getFilterPlan(), Arrays.asList(""));
        extractor.visit();
        //Assert.assertTrue(extractor.canPushDown()); // when changed to filter predicates able to pushdown

    }
}
