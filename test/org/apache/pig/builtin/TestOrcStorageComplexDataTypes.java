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
package org.apache.pig.builtin;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.orc.OrcUtils;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOrcStorageComplexDataTypes {
    final protected static Log LOG = LogFactory.getLog(TestOrcStorageComplexDataTypes.class);

    final private static String basedir = "test/org/apache/pig/builtin/orc/";
    final private static String outbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";

    private static String INPUT1 = outbasedir + "TestOrcStorage_1";
    private static String INPUT2 = outbasedir + "TestOrcStorage_2";
    private static String OUTPUT1 = outbasedir + "TestOrcStorage_out1";
    private static String OUTPUT2 = outbasedir + "TestOrcStorage_out2";
    private static String OUTPUT3 = outbasedir + "TestOrcStorage_out3";
    private static String OUTPUT4 = outbasedir + "TestOrcStorage_out4";

    private static PigServer pigServer = null;
    private static FileSystem fs;

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        fs = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
        pigServer.mkdirs(outbasedir);
        generateInputFiles();
        if (Util.WINDOWS) {
            INPUT1 = INPUT1.replace("\\", "/");
            INPUT2 = INPUT2.replace("\\", "/");
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
            OUTPUT4 = OUTPUT4.replace("\\", "/");
        }
    }

    @After
    public void teardown() throws IOException {
        if (pigServer != null) {
            pigServer.shutdown();
        }
        deleteTestFiles();
    }

    private void generateInputFiles() throws IOException {
        String[] input = {"65536\tworld", "1\thello"};
        Util.createLocalInputFile(INPUT1, input);
        
    }

    private static void deleteTestFiles() throws IOException {
        Util.deleteDirectory(new File(outbasedir));
    }
 
    @Test
    public void testBagOfTuples() throws Exception {

        Tuple[] tuple = new Tuple[3];
        for (int i = 0;i<3;i++) {
            //create tuple objects
            List<Object> tupleObjects = new ArrayList<Object>();
            tupleObjects.add("Hello");
            tupleObjects.add("World");
            tupleObjects.add(i);
            tuple[i] = TupleFactory.getInstance().newTuple(tupleObjects);
        }
        // create the bag of tuples
        DataBag bag = Util.createBag(tuple);
        PigFile f = new PigFile(INPUT2);
        f.store(bag, new FuncSpec(PigStorage.class.getCanonicalName()),
                pigServer.getPigContext());
        
        pigServer.registerQuery("A = load '" + INPUT2 + "' as (a0:chararray, a1:chararray, a2:int);");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(3, reader.getNumberOfRows());

        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());
        FieldSchema fs = job.getPOStore().getSchema().getField(0);
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.CHARARRAY, rs.getType());
    }
    
    @Test
    public void testBagOfBags() throws Exception {

        // create tuple
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append(1);
        DataBag bag1 = BagFactory.getInstance().newDefaultBag();
        bag1.add(TupleFactory.getInstance().newTuple("bag" + 1));
        tuple.append(bag1);
        // create outer bag
        DataBag bag = Util.createBag(new Tuple[]{tuple});
        PigFile f = new PigFile(INPUT2);
        f.store(bag, new FuncSpec(PigStorage.class.getCanonicalName()), pigServer.getPigContext());

        pigServer.registerQuery("A = load '" + INPUT2 + "';");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

        FieldSchema fs = job.getPOStore().getSchema().getField(0);
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.BAG, rs.getType());
        assertEquals(DataType.CHARARRAY, OrcUtils.getTypeInfo(rs)); // recursively gets first field of bag

        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
        IntWritable intWritable = (IntWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        Text text = (Text) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 1);
        assertEquals(text.toString(), "bag1");
    }

    @Test
    public void testBagOfTupleOfMap() throws Exception {

        // create tuple objects
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("rose", "red");
        map1.put("violets", "blue");
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(map1);
        Map<Integer, Double> map2 = new HashMap<Integer, Double>();
        map2.put(1, 3.1417);
        map2.put(2, 2.1819);
        t.append(map2);
        
        // create outer bag
        DataBag bag = Util.createBag(new Tuple[]{t});
        PigFile f = new PigFile(INPUT2);
        f.store(bag, new FuncSpec(PigStorage.class.getCanonicalName()), pigServer.getPigContext());

        pigServer.registerQuery("A = load '" + INPUT2 + "';");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

        List<FieldSchema> fs = job.getPOStore().getSchema().getFields();
        assertEquals(2, fs.size());

        ResourceFieldSchema rs = new ResourceFieldSchema(fs.get(0));
        assertEquals(DataType.MAP, rs.getType());
        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
        MapWritable mapWritable = (MapWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertTrue(mapWritable.containsKey("rose"));
        assertEquals(mapWritable.get("rose"), "red");
        
        mapWritable = (MapWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(mapWritable.get(1), 3.1417);
        assertEquals(mapWritable.get(2), 2.1819);
    }

    @SuppressWarnings("unchecked")
    @Test
    public <V, K> void testBagOfTuplesOfMapArray() throws Exception {

        // create tuple objects
        List<Map<K,V>> maps = new ArrayList<Map<K,V>>();
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("rose", "red");
        map1.put("violets", "blue");
        maps.add((Map<K, V>) map1);
        Map<Integer, Double> map2 = new HashMap<Integer, Double>();
        map2.put(1, 3.1417);
        map2.put(2, 2.1819);
        maps.add((Map<K, V>) map2);
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(maps.toArray());
        
        // create outer bag
        DataBag bag = Util.createBag(new Tuple[]{t});
        PigFile f = new PigFile(INPUT2);
        f.store(bag, new FuncSpec(PigStorage.class.getCanonicalName()), pigServer.getPigContext());

        pigServer.registerQuery("A = load '" + INPUT2 + "';");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);

        List<FieldSchema> fs = job.getPOStore().getSchema().getFields();
        assertEquals(1, fs.size()); //single map array of 2 maps

        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());
        StructObjectInspector soi = (StructObjectInspector) oi;
        StructField elementField = soi.getAllStructFieldRefs().get(0);
        assertEquals(Category.LIST,elementField.getFieldObjectInspector().getCategory());

        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        ArrayWritable arrayWritable = (ArrayWritable) soi.getStructFieldData(row, elementField);
        Writable[] mapsInArray = arrayWritable.get();
        assertEquals(2, mapsInArray.length);
        assertTrue(((MapWritable) mapsInArray[0]).containsKey("rose"));
        assertEquals(((MapWritable) mapsInArray[0]).get("rose"), "red");

        assertEquals(((MapWritable) mapsInArray[1]).get(1), 3.1417);
        assertEquals(((MapWritable) mapsInArray[1]).get(2), 2.1819);
    }
    
    @Test
    public void testStoreByteArrary() throws Exception {
        pigServer.registerQuery("A = load '" + INPUT1 + "' as (a0:int, a1:chararray);");
        // TODO edit or change to multi-store
        pigServer.registerQuery("B = foreach A generate a0 as (int)a0, a1 as (list:{(byte2:char)})a1");
        pigServer.store("B", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);
        
        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector)reader.getObjectInspector();
        IntWritable intWritable = (IntWritable)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(0));
        Text text = (Text)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 65536);
        assertEquals(text.toString(), "world");
        
        row = rows.next(null);
        intWritable = (IntWritable)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(0));
        text = (Text)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 1);
        assertEquals(text.toString(), "hello");
    }

}
