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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.orc.OrcUtils;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestOrcStorageComplexDataTypes {
    final protected static Log LOG = LogFactory.getLog(TestOrcStorageComplexDataTypes.class);

    final private static String basedir = "test/org/apache/pig/builtin/orc/";
    final private static String outbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";

    private static String OUTPUT1 = outbasedir + "TestOrcStorage_out1";
    private static String OUTPUT2 = outbasedir + "TestOrcStorage_out2";
    private static String OUTPUT3 = outbasedir + "TestOrcStorage_out3";

    private static PigServer pigServer = null;
    private static FileSystem filesystem;

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        filesystem = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
        pigServer.mkdirs(outbasedir);
        if (Util.WINDOWS) {
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
        }
    }

    @After
    public void teardown() throws IOException {
        if (pigServer != null) {
            pigServer.shutdown();
        }
        deleteTestFiles();
    }

    private static void deleteTestFiles() throws IOException {
        Util.deleteDirectory(new File(outbasedir));
    }

    @Test
    public void testPrimitiveType() throws Exception {

        Data data = resetData(pigServer);
        data.set("input", "a0:chararray, a1:chararray, a2:int", tuple("Hello", "World", 1), tuple("Hello", "World", 2), tuple("Hello", "World", 3));

        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a0:chararray, a1:chararray, a2:int);");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(filesystem, outputFilePath);
        assertEquals(3, reader.getNumberOfRows());

        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());
        FieldSchema fs = job.getPOStore().getSchema().getField(0);
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.CHARARRAY, rs.getType());

        fs = job.getPOStore().getSchema().getField(1);
        rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.CHARARRAY, rs.getType());

        fs = job.getPOStore().getSchema().getField(2);
        rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.INTEGER, rs.getType());
    }

    @Test
    public void testComplexDataTypes() throws Exception {

        // Test Tuples of Bag containing primitive type
        Data data = resetData(pigServer);
        data.set("input", "a0:int, a1:bag{tuple(chararray)}", tuple(1, bag(tuple("content1, content2"))));
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a0:int, a1:bag{tuple(chararray)});");
        ExecJob job = pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(filesystem, outputFilePath);
        ObjectInspector oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

        FieldSchema fs = job.getPOStore().getSchema().getField(1);
        ResourceFieldSchema rs = new ResourceFieldSchema(fs);
        assertEquals(DataType.BAG, rs.getType());
        assertEquals(ListTypeInfo.class, OrcUtils.getTypeInfo(rs).getClass()); // bag is a list of tuples

        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
        IntWritable intWritable = (IntWritable) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        List bagContents = (List) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 1);
        assertEquals(bagContents.get(0).toString(), "{content1, content2}");

        // Test Tuples of Map
        data = resetData(pigServer);
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("rose", "red");
        map1.put("violets", "blue");
        Map<Integer, Double> map2 = new HashMap<Integer, Double>();
        map2.put(1, 3.1417);
        map2.put(2, 2.1819);
        data.set("input", "a0:map[chararray], a1:map[double]", tuple(map1, map2));
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a0:map[chararray], a1:map[double]);");
        job = pigServer.store("A", OUTPUT2, "OrcStorage");
        outputFilePath = new Path(new Path(OUTPUT2), "part-m-00000");
        reader = OrcFile.createReader(filesystem, outputFilePath);
        oi = reader.getObjectInspector();
        assertEquals(ObjectInspector.Category.STRUCT, oi.getCategory());

        List<FieldSchema> fields = job.getPOStore().getSchema().getFields();
        assertEquals(2, fields.size());

        rs = new ResourceFieldSchema(fields.get(0));
        assertEquals(DataType.MAP, rs.getType());
        assertEquals(MapTypeInfo.class, OrcUtils.getTypeInfo(rs).getClass());

        rows = reader.rows(null);
        row = rows.next(null);
        soi = (StructObjectInspector) reader.getObjectInspector();
        Map map = (Map) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertTrue(map.containsKey(new Text("rose")));
        assertEquals(map.get(new Text("rose")), new Text("red"));

        map = (Map) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(1));
        assertEquals(map.get(new Text("1")), new DoubleWritable(3.1417));
        assertEquals(map.get(new Text("2")), new DoubleWritable(2.1819));

        // test Tuples of Bag containing complex type - Map
        data = resetData(pigServer);
        data.set("input", "a0:bag{tuple(map[chararray])}", tuple(bag(tuple(map1), tuple(map2))));
        pigServer.registerQuery("A = load 'input' using mock.Storage() as (a0:bag{tuple(map[chararray])});");
        job = pigServer.store("A", OUTPUT3, "OrcStorage");
        outputFilePath = new Path(new Path(OUTPUT3), "part-m-00000");
        reader = OrcFile.createReader(filesystem, outputFilePath);

        FieldSchema field = job.getPOStore().getSchema().getField(0);
        rs = new ResourceFieldSchema(field);
        assertEquals(DataType.BAG, rs.getType());
        assertEquals(ListTypeInfo.class, OrcUtils.getTypeInfo(rs).getClass()); // bag is a list of tuples
        field = field.schema.getField(0).schema.getField(0);
        rs = new ResourceFieldSchema(field);
        assertEquals(DataType.MAP, rs.getType());
        assertEquals(MapTypeInfo.class, OrcUtils.getTypeInfo(rs).getClass()); // bag contains map

        rows = reader.rows(null);
        assertEquals(1, reader.getNumberOfRows()); //single tuple in turn having bag of 2 tuples

        oi = reader.getObjectInspector();
        soi = (StructObjectInspector) oi;
        row = rows.next(null);
        bagContents = (List) soi.getStructFieldData(row, soi.getAllStructFieldRefs().get(0));
        assertEquals(2, bagContents.size());

        ListObjectInspector listOI = (ListObjectInspector) soi.getAllStructFieldRefs().get(0).getFieldObjectInspector();
        soi = (StructObjectInspector) listOI.getListElementObjectInspector();
        assertEquals(Category.MAP, soi.getAllStructFieldRefs().get(0).getFieldObjectInspector().getCategory());
    }

    @Test
    public void testHandleByteArrayToBinary() throws Exception {

        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage()"
                + " as (boolean1,byte1,short1,int1,long1,float1,double1,bytes1,string1,"
                + "middle: (list: {(int1,string1)}),list: {(int1,string1)},map: map[(int1,string1)],ts,decimal1);");
        pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(filesystem, outputFilePath);
        assertEquals(7500, reader.getNumberOfRows());

        StructObjectInspector soi = (StructObjectInspector)reader.getObjectInspector();
        StructField sf = soi.getStructFieldRef("bytes1");
        assertEquals(TypeInfoFactory.binaryTypeInfo.getTypeName(), sf.getFieldObjectInspector().getTypeName());

        pigServer.registerQuery("A = load '" + OUTPUT1 + "' using OrcStorage();");
        ExecJob job = pigServer.store("A", OUTPUT2, "PigStorage()");
        assertEquals(1,job.getStatistics().getNumberSuccessfulJobs());
    }

}
