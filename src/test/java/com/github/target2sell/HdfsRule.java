package com.github.target2sell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class HdfsRule extends TemporaryFolder {

    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;

    @Override
    protected void before() throws Throwable {
        super.before();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, getRoot().getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
    }

    @Override
    protected void after() {
        hdfsCluster.shutdown();
        super.after();
    }

    public String getHdfsURI() {
        return hdfsURI;
    }

    public void clear() throws IOException {
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
        fileSystem.delete(new Path("/"), true);
    }
}
