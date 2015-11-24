package com.github.target2sell;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.immutable.Map;
import spark.jobserver.io.JarInfo;
import spark.jobserver.io.JobDAO;
import spark.jobserver.io.JobInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.readAllBytes;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JobCassandraDaoTest {
    private static final byte[] JAR_CONTENT = "Jar content".getBytes(UTF_8);

    private static JobDAO jobDAO;
    private static String datacenter;
    private static String keyspace;
    private static String contactPoint;

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    @ClassRule
    public static CassandraCqlRule cassandraCqlRule = new CassandraCqlRule("jobserver.cql", "jobserver");

    @BeforeClass
    public static void before() throws Exception {
        datacenter = "datacenter1";
        keyspace = "jobserver";
        contactPoint = "127.0.0.1";
        Config config = ConfigFactory.empty().withValue("spark.jobserver.cassandradao.datacenter", ConfigValueFactory.fromAnyRef(datacenter))
                .withValue("spark.jobserver.cassandradao.keyspace", ConfigValueFactory.fromAnyRef(keyspace))
                .withValue("spark.jobserver.cassandradao.contactsPoints", ConfigValueFactory.fromAnyRef(contactPoint))
                .withValue("spark.jobserver.cassandradao.jarCache", ConfigValueFactory.fromAnyRef(temporaryFolder.newFolder().toString()));

        jobDAO = new JobCassandraDao(config);
    }

    @Before
    public void cleanData() {
        Session session = cassandraCqlRule.getSession();
        temporaryFolder.delete();

        Collection<TableMetadata> tables = session.getCluster().getMetadata().getKeyspace(keyspace).getTables();
        for (TableMetadata table : tables) {
            session.execute(QueryBuilder.truncate(table));
        }
    }

    @Test
    public void saveAndRetriveJar() throws Exception {
        String appName = "appName";
        DateTime uploadTime = DateTime.now();

        assertThat(jobDAO.retrieveJarFile(appName, uploadTime), isEmptyString());

        jobDAO.saveJar(appName, uploadTime, JAR_CONTENT);

        String jarPath = jobDAO.retrieveJarFile(appName, uploadTime);
        assertThat(jarPath, not((isEmptyString())));

        Path jarFile = Paths.get(jarPath);
        assertThat(exists(jarFile), is(true));
        assertThat(readAllBytes(jarFile), is(JAR_CONTENT));
    }

    @Test
    public void saveAndReadJobInfos() throws Exception {
        // Read unknown jobId
        assertThat(jobDAO.getJobInfos().isEmpty(), is(true));

        JobInfo jobInfo = createJobInfo("job1");
        jobDAO.saveJobInfo(jobInfo);

        Map<String, JobInfo> jobInfoPersisted = jobDAO.getJobInfos();
        assertThat(jobInfoPersisted.contains("job1"), is(true));
        assertThat(jobInfoPersisted.get("job1").get(), is(jobInfo));


        JobInfo jobInfo2 = createJobInfo("job2");
        jobDAO.saveJobInfo(jobInfo2);
        Map<String, JobInfo> jobInfos2 = jobDAO.getJobInfos();
        assertThat(jobInfos2.size(), is(2));
        assertThat(jobInfos2.contains("job1"), is(true));
        assertThat(jobInfos2.contains("job2"), is(true));
    }

    @Test
    public void saveAndReadJobConfigs() throws Exception {
        assertThat(jobDAO.getJobConfigs().isEmpty(), is(true));

        Config config1 = createConfig();

        jobDAO.saveJobConfig("job1", config1);

        Map<String, Config> jobConfigs = jobDAO.getJobConfigs();
        assertThat(jobConfigs.size(), is(1));
        assertThat(jobConfigs.get("job1").get(), is(config1));
    }

    @Test
    public void getApps() throws Exception {
        assertThat(jobDAO.getApps().isEmpty(), is(true));

        String appName = "appName";
        DateTime uploadTime = DateTime.now();
        jobDAO.saveJar(appName, uploadTime, JAR_CONTENT);

        Map<String, DateTime> apps = jobDAO.getApps();
        assertThat(apps.size(), is(1));
        assertThat(apps.contains(appName), is(true));
        assertThat(apps.get(appName), is(Option.apply(uploadTime)));
    }

    private Config createConfig() {
        return ConfigFactory.empty()
                .withValue("field1", fromAnyRef("value1"))
                .withValue("field2", fromAnyRef("value2"));
    }

    private JobInfo createJobInfo(String id) {
        return new JobInfo(id,
                "contextName",
                new JarInfo("jarName", DateTime.now().withZone(DateTimeZone.UTC)),
                "classpath",
                DateTime.now().withZone(DateTimeZone.UTC),
                Option.<DateTime>empty(),
                Option.<Throwable>empty());
    }


}
