/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.builtin.orc;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.orc.OrcUtils;
import org.apache.pig.newplan.FilterExtractor;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOrcStorage {
    final protected static Log LOG = LogFactory.getLog(TestOrcStorage.class);

    final private static String BASEDIR = "test/org/apache/pig/builtin/orc/";
    final private static String OUTBASEDIR = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";
    final private static String OUTPUT = OUTBASEDIR + "TestOrcStorage_out1";

    private static String INPUT1 = OUTBASEDIR + "TestOrcStorage_1";
    private static String INPUT2 = BASEDIR + "data";
    private static String OUTPUT1 = OUTBASEDIR + "TestOrcStorage_2";
    private static String OUTPUT2 = OUTBASEDIR + "TestOrcStorage_3";
    private static String OUTPUT3 = OUTBASEDIR + "TestOrcStorage_4";
    private static String OUTPUT4 = OUTBASEDIR + "TestOrcStorage_5";
    private static String ORCFILE = OUTBASEDIR + "orcout";

    private static PigServer pigServer = null;
    private static FileSystem fs;
    private File logFile;

    private Driver driver;
    private static final String TEST_WAREHOUSE_DIR = BASEDIR + "warehouse";
    private static final String PARTITIONED_TABLE = "partitioned_table";
    private static final String SIMPLE_TABLE = "non_partitioned_table";
    MiniGenericCluster cluster;

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        fs = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
        pigServer.mkdirs(OUTBASEDIR);
        generateInputFiles();
        if (Util.WINDOWS) {
            INPUT1 = INPUT1.replace("\\", "/");
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
            OUTPUT4 = OUTPUT4.replace("\\", "/");
        }
        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);
    }

    @After
    public void teardown() throws IOException {
        pigServer.shutdown();
        if (cluster != null)
            cluster.shutDown();
        deleteTestFiles();
    }

    private void generateInputFiles() throws IOException {
        String[] input = {"65536\tworld", "1\thello"};
        Util.createLocalInputFile(INPUT1, input);
    }

    private static void deleteTestFiles() throws IOException {
        Util.deleteDirectory(new File(OUTBASEDIR));
    }

    @Test
    public void testSimpleLoad() throws Exception {
        pigServer.registerQuery("A = load '" + BASEDIR + "orc-file-11-format.orc'" + " using OrcStorage();");
        Schema s = pigServer.dumpSchema("A");
        assertEquals(s.toString(), "{boolean1: boolean,byte1: int,short1: int,int1: int,long1: long,"
                + "float1: float,double1: double,bytes1: bytearray,string1: chararray,"
                + "middle: (list: {(int1: int,string1: chararray)}),list: {(int1: int,string1: chararray)},"
                + "map: map[(int1: int,string1: chararray)],ts: datetime,decimal1: bigdecimal}");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        int count = 0;
        Tuple t;
        while (iter.hasNext()) {
            t = (Tuple) iter.next();
            assertEquals(t.size(), 14);
            assertTrue(t.get(0) instanceof Boolean);
            assertTrue(t.get(1) instanceof Integer);
            assertTrue(t.get(2) instanceof Integer);
            assertTrue(t.get(3) instanceof Integer);
            assertTrue(t.get(4) instanceof Long);
            assertTrue(t.get(5) instanceof Float);
            assertTrue(t.get(6) instanceof Double);
            assertTrue(t.get(7) instanceof DataByteArray);
            assertTrue(t.get(8) instanceof String);
            assertTrue(t.get(9) instanceof Tuple);
            assertTrue(t.get(10) instanceof DataBag);
            assertTrue(t.get(11) instanceof Map);
            assertTrue(t.get(12) instanceof DateTime);
            assertTrue(t.get(13) instanceof BigDecimal);
            count++;
        }
        assertEquals(count, 7500);
    }

    @Test
    public void testJoinWithPruning() throws Exception {
        pigServer.registerQuery("A = load '" + BASEDIR + "orc-file-11-format.orc'" + " using OrcStorage();");
        pigServer.registerQuery("B = foreach A generate int1, string1;");
        pigServer.registerQuery("C = order B by int1;");
        pigServer.registerQuery("D = limit C 10;");
        pigServer.registerQuery("E = load '" + INPUT1 + "' as (e0:int, e1:chararray);");
        pigServer.registerQuery("F = join D by int1, E by e0;");
        Iterator<Tuple> iter = pigServer.openIterator("F");
        int count = 0;
        Tuple t = null;
        while (iter.hasNext()) {
            t = iter.next();
            assertEquals(t.size(), 4);
            count++;
        }
        assertEquals(count, 10);
    }

    @Test
    public void testSimpleStore() throws Exception {
        pigServer.registerQuery("A = load '" + INPUT1 + "' as (a0:int, a1:chararray);");
        pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);

        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
        IntWritable intWritable = (IntWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        Text text = (Text) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 65536);
        assertEquals(text.toString(), "world");

        row = rows.next(null);
        intWritable = (IntWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        text = (Text) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 1);
        assertEquals(text.toString(), "hello");

        // A bug in ORC InputFormat does not allow empty file in input directory
        fs.delete(new Path(OUTPUT1, "_SUCCESS"), true);

        // Read the output file back
        pigServer.registerQuery("A = load '" + OUTPUT1 + "' using OrcStorage();");
        Schema s = pigServer.dumpSchema("A");
        assertEquals(s.toString(), "{a0: int,a1: chararray}");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        Tuple t = iter.next();
        assertEquals(t.size(), 2);
        assertEquals(t.get(0), 65536);
        assertEquals(t.get(1), "world");

        t = iter.next();
        assertEquals(t.size(), 2);
        assertEquals(t.get(0), 1);
        assertEquals(t.get(1), "hello");

        assertFalse(iter.hasNext());
    }

    @Test
    public void testMultiStore() throws Exception {
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load '" + INPUT1 + "' as (a0:int, a1:chararray);");
        pigServer.registerQuery("B = order A by a0;");
        pigServer.registerQuery("store B into '" + OUTPUT2 + "' using OrcStorage();");
        pigServer.registerQuery("store B into '" + OUTPUT3 + "' using OrcStorage('-c SNAPPY');");
        pigServer.executeBatch();

        Path outputFilePath = new Path(new Path(OUTPUT2), "part-r-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);
        assertEquals(reader.getCompression(), CompressionKind.ZLIB);

        outputFilePath = new Path(new Path(OUTPUT3), "part-r-00000");
        reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);
        assertEquals(reader.getCompression(), CompressionKind.SNAPPY);
    }

    @Test
    public void testLoadStoreMoreDataType() throws Exception {
        pigServer.registerQuery("A = load '" + BASEDIR + "orc-file-11-format.orc'" + " using OrcStorage();");
        pigServer.registerQuery("B = foreach A generate boolean1..double1, '' as bytes1, string1..;");
        pigServer.store("B", OUTPUT4, "OrcStorage");

        // A bug in ORC InputFormat does not allow empty file in input directory
        fs.delete(new Path(OUTPUT4, "_SUCCESS"), true);

        pigServer.registerQuery("A = load '" + OUTPUT4 + "' using OrcStorage();");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        Tuple t = iter.next();
        assertTrue(t.toString().startsWith(
                "(false,1,1024,65536,9223372036854775807,1.0,-15.0,"
                        + ",hi,({(1,bye),(2,sigh)}),{(3,good),(4,bad)},[],"));
        assertTrue(t.get(12).toString().matches("2000-03-12T00:00:00.000.*"));
        assertTrue(t.toString().endsWith(",12345678.6547456)"));
    }

    @Test
    public void testMultipleLoadStore() throws Exception {
        pigServer.registerQuery("A = load '" + BASEDIR + "orc-file-11-format.orc'" + " using OrcStorage();");
        ExecJob job1 = pigServer.store("A", OUTPUT1, "OrcStorage");
        pigServer.registerQuery("C = load '" + OUTPUT1 + "' using OrcStorage();");
        ExecJob job2 = pigServer.store("C", OUTPUT2, "PigStorage()");
        assertEquals(2, job1.getStatistics().getNumberSuccessfulJobs() + job2.getStatistics().getNumberSuccessfulJobs());
    }

    @Test
    public void testPrimitiveDataTypes() throws Exception {

        String schemaStr = "c: chararray,i: int,l: long,f: float,d: double,b: boolean,dt: datetime,ba,bi: biginteger,bd: bigdecimal";
        Data data = resetData(pigServer);
        DateTime dt = new DateTime();
        data.set(
                "input",
                schemaStr,
                tuple("Hello", 1, (long) 100000, (float) 3.1417, (double) 1234567.8910, true,
                        dt, "World".getBytes(), new BigInteger("12345678901234567890"),
                        new BigDecimal("12345678901234567890.1234567890")));

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (" + schemaStr + ");");
        pigServer.store("A",OUTPUT,"OrcStorage");
        pigServer.registerQuery("B = load '" + OUTPUT + "' using OrcStorage();");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        byte tuples = 0;
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(10, t.size());

            Schema schema = pigServer.dumpSchema("B");
            // implicit typecast to bytearray, and orc(hive) implicit handling of both biginteger,bigdecimal as bigdecimal
            schemaStr = schemaStr.replace("ba", "ba: bytearray").replace("biginteger", "bigdecimal");
            assertEquals("{" + schemaStr + "}", schema.toString());

            assertEquals(DataType.CHARARRAY, DataType.findType(t.get(0)));
            assertEquals("Hello", t.get(0));

            assertEquals(DataType.INTEGER, DataType.findType(t.get(1)));
            assertEquals(1, t.get(1));

            assertEquals(DataType.LONG, DataType.findType(t.get(2)));
            assertEquals((long)100000, t.get(2));

            assertEquals(DataType.FLOAT, DataType.findType(t.get(3)));
            assertEquals((float) 3.1417, t.get(3));

            assertEquals(DataType.DOUBLE, DataType.findType(t.get(4)));
            assertEquals((double) 1234567.8910, t.get(4));

            assertEquals(DataType.BOOLEAN, DataType.findType(t.get(5)));
            assertEquals(true, t.get(5));

            assertEquals(DataType.DATETIME, DataType.findType(t.get(6)));
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            assertEquals(df.format(dt.toDate()), df.format(((DateTime)t.get(6)).toDate()));

            assertEquals(DataType.BYTEARRAY, DataType.findType(t.get(7)));
            assertEquals("World", new String(((DataByteArray)t.get(7)).get()));

            // check hive binary
            Reader reader = OrcFile.createReader(fs, new Path(new Path(OUTPUT), "part-m-00000"));
            StructField f = ((StructObjectInspector)reader.getObjectInspector()).getStructFieldRef(schema.getField(7).alias);
            assertEquals(TypeInfoFactory.binaryTypeInfo.getTypeName(), f.getFieldObjectInspector().getTypeName());

            assertEquals(DataType.BIGDECIMAL, DataType.findType(t.get(8)));
            assertEquals(new BigDecimal("12345678901234567890"), t.get(8));

            assertEquals(DataType.BIGDECIMAL, DataType.findType(t.get(9)));
            assertEquals(new BigDecimal("12345678901234567890.123456789"), t.get(9));
            tuples++;
        }
        assertEquals(1, tuples);
    }

    @Test
    public void testPrimitiveOrcStorage() throws Exception {

        String schemaStr = "c:chararray, i:int, l:long, f:float, d:double, b:boolean, dt:datetime, ba, n, bi:biginteger, bd:bigdecimal";
        Data data = resetData(pigServer);
        DateTime dt = new DateTime();
        data.set(
                "input",
                schemaStr,
                tuple("Hello", 1, (long) 100000, (float) 3.1417, (double) 1234567.8910, true,
                        dt, "World".getBytes(), null, new BigInteger("12345678901234567890"),
                        new BigDecimal("12345678901234567890.1234567890")));

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (" + schemaStr + ");");
        ExecJob job = pigServer.store("A", OUTPUT, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(1, reader.getNumberOfRows());

        Object row = reader.rows(null).next(null);
        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<?> tupleData = soi.getStructFieldsDataAsList(row);
        assertEquals(11, tupleData.size());

        int index = 0;
        Schema schema = job.getPOStore().getSchema();
        assertEquals(DataType.CHARARRAY, schema.getField(index).type);
        assertEquals(new Text("Hello"), tupleData.get(index++));

        assertEquals(DataType.INTEGER, schema.getField(index).type);
        assertEquals(1, ((IntWritable)tupleData.get(index++)).get());

        assertEquals(DataType.LONG, schema.getField(index).type);
        assertEquals(100000, ((LongWritable)tupleData.get(index++)).get());

        assertEquals(DataType.FLOAT, schema.getField(index).type);
        assertEquals((float)3.1417, ((FloatWritable)tupleData.get(index++)).get(), (float)0.00001);

        assertEquals(DataType.DOUBLE, schema.getField(index).type);
        assertEquals(1234567.8910, ((DoubleWritable)tupleData.get(index++)).get(), 0.00001);

        assertEquals(DataType.BOOLEAN, schema.getField(index).type);
        assertEquals(true, ((BooleanWritable)tupleData.get(index++)).get());

        assertEquals(DataType.DATETIME, schema.getField(index).type);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        assertEquals(df.format(dt.toDate()), df.format(tupleData.get(index++)));

        assertEquals(DataType.BYTEARRAY, schema.getField(index).type);
        StructField sf = soi.getStructFieldRef(schema.getField(index).alias);
        assertEquals(TypeInfoFactory.binaryTypeInfo.getTypeName(), sf.getFieldObjectInspector().getTypeName());
        assertEquals("World", new String(((BytesWritable)tupleData.get(index++)).getBytes()).trim());

        assertEquals(DataType.BYTEARRAY, schema.getField(index).type); // null is loaded into schema as bytearray
        assertNull(tupleData.get(index++));

        assertEquals(DataType.BIGINTEGER, schema.getField(index).type);
        // MAX_PRECISION 38, MAX_SCALE 18, therefore maxIntDigits = 38-18=20
        assertEquals(HiveDecimal.create("12345678901234567890"), ((HiveDecimal) tupleData.get(index++)));

        assertEquals(DataType.BIGDECIMAL, schema.getField(index).type);
        // MAX_PRECISION 38, MAX_SCALE 18
        assertEquals(HiveDecimal.create("12345678901234567890.1234567890"), ((HiveDecimal) tupleData.get(index++)));
    }

    @Test
    public void testComplexDataTypes() throws Exception {
        // Test Bag of Tuple of more than one field
        String schema = "b:bag{t:tuple(t0:int, t1:chararray)}";
        Data data = resetData(pigServer);
        Tuple[] input = new Tuple[1];
        input[0] = tuple(bag(tuple(1, "content1"), tuple(2, "content2")));
        data.set("input", schema, input);

        checkWithOrcStorage(schema, input);

        // Test Map
        schema = "m1:map[chararray], m2:map[double]";
        data = resetData(pigServer);
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("rose", "red");
        map1.put("violets", "blue");
        Map<String, Double> map2 = new HashMap<String, Double>();
        map2.put("1", 3.1417);
        map2.put("2", 2.1819);
        input[0] = tuple(map1, map2);
        data.set("input", schema, input);

        checkWithOrcStorage(schema, input);

        // test Bag of Map
        schema = "b:bag{t:tuple(m:map[chararray])}";
        data = resetData(pigServer);
        input[0] = tuple(bag(tuple(map1), tuple(map2)));
        data.set("input", schema, input);

        checkWithOrcStorage(schema, input);

        // test Map of Map
        schema = "m:map[map[chararray]]";
        data = resetData(pigServer);
        Map<String, Map<String, Object>> map3 = new HashMap<String, Map<String, Object>>();
        map3.put("mapkeystring", map1);
        input[0] = tuple(map3);
        data.set("input", schema, input);

        checkWithOrcStorage(schema, input);
    }

    private void checkWithOrcStorage(String schema, Tuple... expected) throws Exception {
        //cleanup
        fs.delete(new Path(OUTPUT), true);

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (" + schema + ");");
        pigServer.store("A", OUTPUT, "OrcStorage");
        pigServer.registerQuery("B = load '" + OUTPUT + "' using OrcStorage();");

        Schema schemaExpected = pigServer.dumpSchema("A");
        Schema schemaActual = pigServer.dumpSchema("B");
        Iterator<Tuple> actual = pigServer.openIterator("B");
        if (!Schema.equals(schemaExpected, schemaActual, false, true)) {
            fail();
        }
        Util.checkQueryOutputs(actual, expected);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testComplexOrcStorage() throws Exception {
        // Test Bag of Tuple of more than one field
        String schema = "b:bag{t:tuple(t0:int, t1:chararray)}";
        Data expectedData = resetData(pigServer);
        Tuple[] input = new Tuple[1];
        input[0] = tuple(bag(tuple(1, "content1"), tuple(2, "content2")));
        expectedData.set("input", schema, input);

        Object actualData = checkOrcTypes(schema, 1, DataType.BAG, (byte)0, (byte)0, input);
        List list = (List) actualData;
        assertEquals("{1, content1}", list.get(0).toString());
        assertEquals("{2, content2}", list.get(1).toString());

        // Test Map
        schema = "a0:map[chararray], a1:map[double]";
        expectedData = resetData(pigServer);
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("rose", "red");
        map1.put("violets", "blue");
        Map<Integer, Double> map2 = new HashMap<Integer, Double>();
        map2.put(1, 3.1417);
        map2.put(2, 2.1819);
        input[0] = tuple(map1, map2);
        expectedData.set("input", schema, input);

        actualData = checkOrcTypes(schema, 2, DataType.MAP, (byte)0, (byte)0, input);
        Map map = (Map) actualData;
        assertTrue(map.containsKey(new Text("rose")));
        assertEquals(map.get(new Text("rose")), new Text("red"));

        // test Bag of Map
        schema = "a0:bag{tuple(map[chararray])}";
        expectedData = resetData(pigServer);
        input[0] = tuple(bag(tuple(map1), tuple(map2)));
        expectedData.set("input", schema, input);

        actualData = checkOrcTypes(schema, 1, DataType.BAG, DataType.MAP, (byte)2, input);
        list = (List) actualData;
        assertEquals(2, list.size());
        assertTrue(list.get(0).toString().contains(map1.toString()));
        //not checking list.get(1) as order of map keys is changed

        // test Map of Map
        schema = "m:map[map[chararray]]";
        expectedData = resetData(pigServer);
        Map<String, Map<String, Object>> map3 = new HashMap<String, Map<String, Object>>();
        map3.put("mapkeystring", map1);
        input[0] = tuple(map3);
        expectedData.set("input", schema, input);

        actualData = checkOrcTypes(schema, 1, DataType.MAP, DataType.MAP, (byte)1, input);
        map = (Map) actualData;
        assertEquals(new Text("mapkeystring"), map.keySet().iterator().next());
        assertEquals(map1.toString(), map.get(new Text("mapkeystring")).toString());
    }

    private Object checkOrcTypes(String schema, int tupleFields, byte expectedType, byte nestedExpectedType,
            byte level, Tuple... expected) throws Exception {
        // cleanup
        fs.delete(new Path(OUTPUT), true);

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (" + schema + ");");
        pigServer.store("A", OUTPUT, "OrcStorage");
        pigServer.registerQuery("B = load '" + OUTPUT + "' using OrcStorage();");

        Schema schemaExpected = pigServer.dumpSchema("A");
        Schema schemaActual = pigServer.dumpSchema("B");
        if (!Schema.equals(schemaExpected, schemaActual, false, true)) {
            fail();
        }

        List<FieldSchema> fields = schemaActual.getFields();
        assertEquals(tupleFields, fields.size());

        Path outputFilePath = new Path(new Path(OUTPUT), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory()); // tuple <==> STRUCT

        FieldSchema fs = schemaActual.getField(0);
        testTypeInfo(fs, expectedType);
        if (nestedExpectedType > 0) {
            for (byte b = level; b > 0 ; b--)
                fs = fs.schema.getField(0);
            testTypeInfo(fs, nestedExpectedType);
        }

        assertEquals(1, reader.getNumberOfRows()); // single top-level tuple having nested other data e.g. bag/map etc
        Object row = reader.rows(null).next(null);
        StructObjectInspector soi = (StructObjectInspector) oi;
        return soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
    }

    private void testTypeInfo(FieldSchema fs, byte expectedType) throws Exception {
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(expectedType, rs.getType());
        @SuppressWarnings("rawtypes")
        Class typeinfo = null;
        switch (expectedType) {
            case DataType.BAG :
                typeinfo = ListTypeInfo.class;
                break;
            case DataType.MAP :
                typeinfo = MapTypeInfo.class;
        }
        assertEquals(typeinfo, OrcUtils.getTypeInfo(rs).getClass()); // bag <==> LIST, map <==> MAP
    }

    /**
     * ORCStorage will load only required columns from HDFS, thus increasing
     * overall efficiency. This test proves this as compared to PigStorage
     * Need PigServer M-R mode for this test to get M-R counters info
     *
     * @throws Exception
     */
    @Test
    public void testColumnPruneBytesRead() throws Exception {
        System.setProperty("test.exec.type", "mr");
        cluster = MiniGenericCluster.buildCluster();
        cluster.setProperty("pig.use.overriden.hadoop.configs", "true");
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        createData();

        pigServer.registerQuery("C = load 'input' as (c0:int, c1:int, c2:int);");
        pigServer.registerQuery("D = foreach C generate c0, c1, c2;");
        ExecJob job = pigServer.store("D", "temp1");
        MRJobStats stats = (MRJobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithPig1 = stats.getHdfsBytesRead();
        pigServer.registerQuery("C = load 'input' as (c0:int, c1:int, c2:int);");
        pigServer.registerQuery("D = foreach C generate c1;");
        job = pigServer.store("D", "temp2");
        stats = (MRJobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithPig2 = stats.getHdfsBytesRead();
        assertEquals(bytesWithPig1, bytesWithPig2); // column pruning does not help in reducing data load

        pigServer.registerQuery("E = load 'orcfile' using OrcStorage();");
        pigServer.registerQuery("F = foreach E generate a0, a1, a2;");
        job = pigServer.store("F", "temp3");
        stats = (MRJobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithOrc1 = stats.getHdfsBytesRead();
        pigServer.registerQuery("E = load 'orcfile' using OrcStorage();");
        pigServer.registerQuery("F = foreach E generate a1;");
        job = pigServer.store("F", "temp4");
        stats = (MRJobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithOrc2 = stats.getHdfsBytesRead();
        assertNotEquals(bytesWithOrc1, bytesWithOrc2);
        assertTrue(bytesWithOrc2 < bytesWithOrc1); // column pruning directly helps in reducing data load

        Iterator<Tuple> iter = job.getResults();
        Tuple[] expected = new Tuple[2];
        expected[0] = tuple(2);
        expected[1] = tuple(5);
        Util.checkQueryOutputs(iter, expected);
        Util.checkLogFileMessage(logFile, new String[]{"Columns pruned for E: $0, $2"}, true);

        cleanupData();
    }

    private void createData() throws Exception {
        String[] input = {"1\t2\t3", "4\t5\t6"};
        Util.createInputFile(pigServer.getPigContext(), "input", input);
        pigServer.registerQuery("A = load 'input' as (a0:int, a1:int, a2:int);");
        pigServer.store("A", "orcfile", "OrcStorage");
    }

    private void cleanupData() throws Exception {
        Util.deleteFile(cluster, "temp1");
        Util.deleteFile(cluster, "temp2");
        Util.deleteFile(cluster, "temp3");
        Util.deleteFile(cluster, "temp4");
        Util.deleteFile(cluster, "input");
        Util.deleteFile(cluster, "orcfile");
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

    /**
     * Test that OrcStorage prunes columns correctly when loading data
     * previously stored via HCatStorer and partition filters work
     *
     * @throws Exception
     */
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
        pigServer.store("A", ORCFILE, "OrcStorage");

        pigServer.registerQuery("B = load '" + ORCFILE + "' using OrcStorage();");
        pigServer.registerQuery("C = filter B by bkt == '0';");
        pigServer.registerQuery("D = foreach C generate a, b;");
        pigServer.store("D", "tmp", "mock.Storage()");
        schema = pigServer.dumpSchema("D");
        assertEquals("{a: chararray,b: int}", schema.toString());
        // columns a,b ($0, $1) required and c,d ($2, $3) pruned
        Util.checkLogFileMessage(logFile, new String[]{"Columns pruned for B: $2, $3"}, true);

        Iterator<Tuple> iter = pigServer.openIterator("D");
        Tuple[] expected = new Tuple[2];
        expected[0] = tuple("string1", 1);
        expected[1] = tuple("string2", 2);
        Util.checkQueryOutputs(iter, expected);

        tearDownHCatalog();
    }

    @SuppressWarnings("unchecked")
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
        + "STORE A INTO '" + ORCFILE + "' USING OrcStorage();";
        pigServer.registerQuery(query);

        query = "B = LOAD '" + ORCFILE + "' USING OrcStorage();"
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
        Util.checkLogFileMessage(logFile, new String[]{"Map key required for B: $2->[empid, name]"}, true);

        List<Tuple> out = data.get("testPrune");
        assertEquals(out.get(0), tuple(1,(float)1.0,mapv1));
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
        pigServer.store("A",INPUT2,"OrcStorage('-r 1000')");

        Configuration conf = ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties());
        FileSystem fs = FileSystem.get(conf);
        Reader reader = OrcFile.createReader(new Path(INPUT2, "part-m-00000"), OrcFile.readerOptions(conf)
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

        String query = "A = load '" + INPUT2 + "' using OrcStorage();"
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
