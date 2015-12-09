package com.github.target2sell;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import scala.Function0;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.immutable.Map;
import spark.jobserver.io.JobDAO;
import spark.jobserver.io.JobInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;
import static scala.collection.JavaConversions.asScalaMap;


public class JobCassandraDao implements JobDAO {
    public static final String ROOT_DIRECTORY = "/jobserver/";
    private final Logger logger = getLogger(JobCassandraDao.class);

    private final ObjectMapper mapper;
    private final Path jarCacheDirectory;
    private final String defaultFS;
    private final String cfsImpl = "com.datastax.bdp.hadoop.cfs.CassandraFileSystem";
    private Session session;

    private PreparedStatement saveJobConfigStatement;
    private PreparedStatement findJobConfigStatement;
    private PreparedStatement saveJobInfoStatement;
    private PreparedStatement findJobInfosStatement;
    private final FileSystem fs;


    public JobCassandraDao(Config config) throws IOException {
        Config defaultConfig = ConfigFactory.empty()
                .withValue("spark.jobserver.cassandradao.datacenter", fromAnyRef("dc1"))
                .withValue("spark.jobserver.cassandradao.keyspace", fromAnyRef("jobserver"))
                .withValue("spark.jobserver.cassandradao.contactsPoints", fromAnyRef("127.0.0.1"))
                .withValue("spark.jobserver.cassandradao.consistencyLevel", fromAnyRef("ONE"))
                .withValue("spark.jobserver.cassandradao.datacenter", fromAnyRef("datacenter1"))
                .withValue("spark.jobserver.cassandradao.jarCache", fromAnyRef("/tmp/jobserver/cassandradao/"))
                .withValue("spark.jobserver.cassandradao.fsUri", fromAnyRef("cfs://127.0.0.1/"));

        Config configMerged = config.withFallback(defaultConfig);
        String datacenter = configMerged.getString("spark.jobserver.cassandradao.datacenter");
        String keyspace = configMerged.getString("spark.jobserver.cassandradao.keyspace");
        String contactsPoints = configMerged.getString("spark.jobserver.cassandradao.contactsPoints");
        String consistencyLevel = configMerged.getString("spark.jobserver.cassandradao.consistencyLevel");
        defaultFS = configMerged.getString("spark.jobserver.cassandradao.fsUri");

        SessionProvider sessionProvider = new SessionProvider(datacenter, keyspace, contactsPoints, consistencyLevel);
        session = sessionProvider.getInstance();

        this.jarCacheDirectory = Paths.get(configMerged.getString("spark.jobserver.cassandradao.jarCache"));
        init();

        mapper = new ObjectMapper();
        mapper.registerModules(new DefaultScalaModule(), new JodaModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        fs = FileSystem.get(getFSConfig());
    }

    private void init() {
        saveJobInfoStatement = session.prepare("INSERT INTO job_info(job_id, content) VALUES (:job_id, :content)");
        findJobInfosStatement = session.prepare("SELECT job_id, content FROM job_info");

        saveJobConfigStatement = session.prepare("INSERT INTO job_config(job_id, content) VALUES (:job_id, :content)");
        findJobConfigStatement = session.prepare("SELECT job_id, content FROM job_config");
    }


    @Override
    public void saveJar(String appName, DateTime uploadTime, byte[] jarBytes) {
        try {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(ROOT_DIRECTORY + uploadTime.getMillis() + "/" + appName + ".jar");
            try (FSDataOutputStream dataOutputStream = fs.create(path)) {
                dataOutputStream.write(jarBytes);
            }
        } catch (IOException e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
        }
    }

    private org.apache.hadoop.conf.Configuration getFSConfig() {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();

        config.set("fs.default.name", defaultFS);
        config.set("fs.cfs.impl", cfsImpl);
        return config;
    }

    @Override
    public Map<String, DateTime> getApps() {
        java.util.Map<String, DateTime> result = new java.util.HashMap<>();

        try {
            final org.apache.hadoop.fs.Path jarDir = new org.apache.hadoop.fs.Path(ROOT_DIRECTORY);
            if (!fs.exists(jarDir)) {
                return convertMapToImmutableMap(result);
            }

            for (FileStatus directoryTimestamp : fs.listStatus(jarDir)) {
                if (!directoryTimestamp.isDir()) {
                    continue;
                }
                for (FileStatus jarPath : fs.listStatus(directoryTimestamp.getPath())) {
                    String jarName = jarPath.getPath().getName();
                    if (!jarName.endsWith(".jar")) {
                        continue;
                    }

                    String appname = jarName.replaceAll("\\.jar$", "");
                    long dateTime = Long.parseLong(jarPath.getPath().getParent().getName());
                    result.put(appname, new DateTime(dateTime));
                }
            }
        } catch (IOException e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
        }

        return convertMapToImmutableMap(result);
    }

    @Override
    public String retrieveJarFile(String appName, DateTime uploadTime) {
        Path jar = jarCacheDirectory.resolve(Paths.get(uploadTime.toString(), appName + ".jar"));

        boolean jarExists = jar.toFile().exists() || fetchJarToCacheFile(appName, uploadTime, jar);

        if (!jarExists) return "";
        return jar.toAbsolutePath().toString();
    }

    private boolean fetchJarToCacheFile(String appName, DateTime uploadTime, Path jar) {
        if (!jar.getParent().toFile().exists() && !jar.getParent().toFile().mkdirs()) {
            logger.error("Unable to make jar cache directory in: {}", jar.getParent().toString());
            return false;
        }
        try {
            org.apache.hadoop.fs.Path srcJar = new org.apache.hadoop.fs.Path(ROOT_DIRECTORY + uploadTime.getMillis() + "/" + appName + ".jar");
            org.apache.hadoop.fs.Path localJar = new org.apache.hadoop.fs.Path(jar.toAbsolutePath().toString());
            if (!fs.exists(srcJar)) {
                return false;
            }
            fs.copyToLocalFile(srcJar, localJar);
        } catch (IOException e) {
            logger.error("Unable to write jar content: {}", e.getMessage(), e);
            return false;
        }

        return true;
    }

    @Override
    public void saveJobInfo(JobInfo jobInfo) {
        try {
            String content = mapper.writeValueAsString(jobInfo);
            session.execute(saveJobInfoStatement.bind(jobInfo.jobId(), content));
        } catch (JsonProcessingException e) {
            logger.error("Unable to parse serialize jobinfo: {}", e.getMessage(), e);
            throw new RuntimeException("Unable to parse serialize jobinfo: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, JobInfo> getJobInfos() {
        final ResultSet resultSet = session.execute(findJobInfosStatement.bind());

        java.util.Map<String, JobInfo> jobInfoMap = StreamSupport.stream(resultSet.spliterator(), false)
                .map(this::parseStringToJobInfo)
                .filter(jobInfo -> jobInfo != null)
                .collect(toMap(JobInfo::jobId, jobInfo -> jobInfo));

        return convertMapToImmutableMap(jobInfoMap);
    }

    private JobInfo parseStringToJobInfo(Row row) {
        try {
            return mapper.readValue(row.getString("content"), JobInfo.class);
        } catch (IOException e) {
            logger.error("Unable to parse jobinfo: {}", e.getMessage(), e);
            throw new RuntimeException("Unable to parse jobinfo: " + e.getMessage(), e);
        }
    }

    @Override
    public void saveJobConfig(String jobId, Config jobConfig) {
        String content = jobConfig.root().render(ConfigRenderOptions.concise());
        session.execute(saveJobConfigStatement.bind(jobId, content));
    }

    @Override
    public Map<String, Config> getJobConfigs() {
        ResultSet resultSet = session.execute(findJobConfigStatement.bind());

        java.util.Map<String, Config> jobConfigs = StreamSupport.stream(resultSet.spliterator(), false)
                .map(this::parseStringToJobConfig)
                .filter(jobConfig -> jobConfig != null)
                .collect(toMap(Tuple2::_1, Tuple2::_2));

        return convertMapToImmutableMap(jobConfigs);
    }

    private <T, U> Map<T, U> convertMapToImmutableMap(java.util.Map<T, U> javaMap) {
        return asScalaMap(javaMap).toMap(Predef.<Tuple2<T, U>>conforms());
    }

    private Tuple2<String, Config> parseStringToJobConfig(Row row) {
        return new Tuple2<>(row.getString("job_id"), ConfigFactory.parseString(row.getString("content")));
    }

    @Override
    public Option<DateTime> getLastUploadTime(String appName) {
        return this.getApps().get(appName);
    }

    @Override
    public <T> T getOrElse(Function0<T> getter, T _default) {
        return Optional.of(getter.apply()).orElse(_default);
    }
}
