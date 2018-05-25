package org.vivoweb.tools;

import org.apache.jena.riot.RDFFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 * @author awoods
 * @since 2018-05-25
 */
public class ApplicationStoresTest {

    private File testDir;
    private File dumpDir;

    private ApplicationStores stores;

    @Before
    public void setUp() {
        URL url = this.getClass().getResource("/");
        testDir = new File(url.getFile());
        dumpDir = new File(testDir, "dumps");
        if (!dumpDir.exists()) {
            Assert.assertTrue("Unable to create 'dumps'!", dumpDir.mkdir());
        }
    }

    @Test
    public void writeContent_1_9_3() {
        String homePath = testDir.getPath() + "/home-1.9.3";
        stores = new ApplicationStores(homePath, RDFFormat.TRIG_BLOCKS);

        File output = new File(dumpDir, "content.1.9.3");
        stores.writeContent(output);

        Assert.assertTrue("tdbModels were not created!", new File(homePath, "tdbModels").isDirectory());
    }

    @Test
    public void writeContent_1_10_0() {
        String homePath = testDir.getPath() + "/home-1.10.0";
        stores = new ApplicationStores(homePath, RDFFormat.TRIG_BLOCKS);

        File output = new File(dumpDir, "content.1.10.0");
        stores.writeContent(output);

        Assert.assertTrue("tdbModels were not created!", new File(homePath, "tdbModels").isDirectory());
    }
}