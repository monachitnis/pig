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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.bag;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
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
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.orc.OrcUtils;
import org.apache.pig.test.Util;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestOrcStorageAllDataTypes {

    private static PigServer pigServer = null;
    private static FileSystem filesystem;

    final private static String BASEDIR = "test/org/apache/pig/builtin/orc/";
    final private static String OUTBASEDIR = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";
    final private static String OUTPUT = OUTBASEDIR + "TestOrcStorage_out1";

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        filesystem = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
    }

    @After
    public void teardown() throws IOException {
        pigServer.shutdown();
        deleteTestFiles();
    }

    private static void deleteTestFiles() throws IOException {
        Util.deleteDirectory(new File(OUTBASEDIR));
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
            Reader reader = OrcFile.createReader(filesystem, new Path(new Path(OUTPUT), "part-m-00000"));
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
        Reader reader = OrcFile.createReader(filesystem, outputFilePath);
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
        filesystem.delete(new Path(OUTPUT), true);

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
        filesystem.delete(new Path(OUTPUT), true);

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
        Reader reader = OrcFile.createReader(filesystem, outputFilePath);
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
}
