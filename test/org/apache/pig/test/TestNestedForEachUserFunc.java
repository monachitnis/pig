package org.apache.pig.test;

import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNestedForEachUserFunc extends junit.framework.TestCase {
    private PigServer pig;

    public TestNestedForEachUserFunc() throws Exception {
        pig = new PigServer(ExecType.LOCAL, new Properties());
        pig.setValidateEachStatement(true);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        String[] input = {"1\tFrodo\tBaggins\tHobbbit", "2\tSam\tGamgee\tHobbit", "3\tGandalf\tGrey\tWizard"};
        //newUtil.createInputFile(FileSystem.getLocal(new Configuration()), "my_data", input);

    }

    @Override
    @After
    public void tearDown() throws Exception {
        FileSystem.getLocal(new Configuration()).delete(new Path("my_data"), true);
    }

    @Test
    public void testSimple() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1:int, col2:chararray, col3:chararray, col4:chararray);\n");
        pig.registerQuery("b = foreach a { c1 = UPPER(col2); generate (c1 eq 'FRODO' ? 1 : 0), (c1 eq 'SAM' ? 2 : 0); }\n");

        Iterator<Tuple> iter = pig.openIterator("b");
        String[] expected = new String[]{"(1,0)", "(0,2)", "(0,0)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("b")));

    }

    /*@Test
    public void testComplex() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1:int, col2:chararray, col3:chararray, col4:chararray);\n");
        pig.registerQuery("b = foreach a { x = TOBAG(TOTUPLE(col1, col2));"
                                        + "y = UPPER(col3); "
                                        + "generate col1, (x.$1.$0 eq 'Frodo' ? 1 : 0), (x.$1.$0 eq 'Sam' ? 2 : 0), y, LOWER(col4); }\n");

        Iterator<Tuple> iter = pig.openIterator("b");
        String[] expected = new String[]{"(1,1,0,BAGGINS,hobbit)",
                                         "(2,0,2,GAMGEE,hobbit)",
                                         "(3,0,0,GREY,wizard)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("b")));

    }
*/
    @Test
    public void testCastWithinUserFunc() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1, col2, col3, col4);\n");
        pig.registerQuery("b = foreach a { c1 = UPPER((chararray)col2); generate (c1 eq 'FRODO' ? 1 : 0), (c1 eq 'SAM' ? 2 : 0); }\n");

        Iterator<Tuple> iter = pig.openIterator("b");
        String[] expected = new String[]{"(1,0)", "(0,2)", "(0,0)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("b")));
    }

    @Test
    public void testIndex() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1:int, col2:chararray, col3:chararray, col4:chararray);\n");
        pig.registerQuery("b = foreach a { c1 = UPPER($1); generate (c1 eq 'FRODO' ? 1 : 0), (c1 eq 'SAM' ? 2 : 0), LOWER($2); }\n");

        Iterator<Tuple> iter = pig.openIterator("b");
        String[] expected = new String[]{"(1,0,baggins)", "(0,2,gamgee)", "(0,0,grey)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("b")));
    }

    @Test
    public void testScalars() throws Exception {

        String[] input = {"4\tAragorn\tII\tMan"};
        Util.createInputFile(FileSystem.getLocal(new Configuration()), "one_data", input);

        pig.registerQuery("a = load 'my_data' as (a1,a2,a3,a4);\n");
        pig.registerQuery("b = load 'one_data' as (b1,b2,b3,b4);\n");
        pig.registerQuery("c = foreach a { c1 = CONCAT((chararray)a2, (chararray)b.b2); generate c1, (c1 eq 'FrodoAragorn' ? 1 : 0); }\n");

        Iterator<Tuple> iter = pig.openIterator("c");
        String[] expected = new String[]{"(FrodoAragorn,1)", "(SamAragorn,0)", "(GandalfAragorn,0)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("c")));

        FileSystem.getLocal(new Configuration()).delete(new Path("one_data"), true);

    }

    /*@Test
    public void testGroupByWithinNested() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1, col2, col3, col4);\n");
        pig.registerQuery("b = foreach a { c = grou
                + "c1 = UPPER((chararray)col2); generate (c1 eq 'FRODO' ? 1 : 0), (c1 eq 'SAM' ? 2 : 0); }\n");
    }
*/
    @Test
    public void testConstantArg() throws Exception {

        pig.registerQuery("a = load 'my_data' as (col1:int, col2:chararray, col3:chararray, col4:chararray);\n");
        pig.registerQuery("b = foreach a { c1 = STARTSWITH(TOTUPLE(col2,'f')); generate (c1 eq true ? 'frodo' : 'not frodo'), (c1 eq true ? 'frodo' : 'not frodo'); }\n");

        Iterator<Tuple> iter = pig.openIterator("b");
        String[] expected = new String[]{"(frodo,not sam)", "(not frodo,sam)", "(not frodo,not sam)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("b")));
    }

}
