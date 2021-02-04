package cc.schut.yarnstats;

import cc.schut.yarnstats.service.YarnService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class MainApplication implements CommandLineRunner {

    @Autowired
    private YarnService yarnService;

    public static void main(String[] args) {
        new SpringApplicationBuilder(MainApplication.class)
                .bannerMode(Banner.Mode.OFF)
                .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        yarnService.calcYarnPercentiles();
    }

}
