package cc.schut.yarnstats.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class YarnService {
    private static Logger LOG = LoggerFactory.getLogger(YarnService.class);

    private Configuration conf = new YarnConfiguration();

    @PostConstruct
    public void init() throws IOException {
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        UserGroupInformation.setConfiguration(conf);
    }

    public void calcYarnPercentiles() {
        LOG.info("Starting yarn application fetching");

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        try {
            long now = System.currentTimeMillis();
            Set users = new HashSet<String>() {
                {
                    add("mapred");
                }
            };

            List<ApplicationReport> applications = yarnClient.getApplications(
                    null, users, null,
                    EnumSet.of(YarnApplicationState.RUNNING,
                                YarnApplicationState.ACCEPTED,
                                YarnApplicationState.SUBMITTED));
            List<Long> durations = new ArrayList(applications.size());
            applications.forEach(report -> {
                long duration = now - report.getStartTime();
                durations.add(duration);
            });
            double percentile = 90;
            LOG.info("Found {} running applications. {} minutes {} percentile", applications.size(), percentile(durations, percentile) / 1000 / 60, percentile);
        } catch (YarnException | IOException e) {
            LOG.error("Error while reading a list of applications", e);
        }
    }

    public static long percentile(List<Long> latencies, double percentile) {
        Collections.sort(latencies);
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }
}
