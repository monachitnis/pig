package org.apache.pig.test;

import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.junit.Test;

public class TestNestedForEachUserFunc {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    @Test
    public void testSimple() throws Exception {

    }
}
