package com.github.target2sell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class HdfsRule extends TemporaryFolder {

    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;

    @Override
    protected void before() throws Throwable {
        super.before();
        getRoot().getAbsoluteFile().mkdirs();
        Configuration conf = new Configuration();
        conf.set("dfs.name.dir", getRoot().getAbsolutePath());
        hdfsCluster = new MiniDFSCluster(conf, 1, true, null);

        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
    }

    @Override
    protected void after() {
        hdfsCluster.shutdownNameNode();
        hdfsCluster.shutdownDataNodes();
        hdfsCluster.shutdown();

        super.after();
    }

    public String getHdfsURI() {
        return hdfsURI;
    }

    public void clear() throws IOException {
        hdfsCluster.getFileSystem().delete(new Path("/"), true);
    }
}
