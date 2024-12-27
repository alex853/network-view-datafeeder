package net.simforge.networkview.datafeeder;

import net.simforge.commons.misc.Misc;
import net.simforge.commons.runtime.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Arrays;

@SpringBootApplication
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) throws IOException {
        if (Arrays.asList(args).contains("k:stop")) {
            logger.info("k:stop signal found in params, setting stop signal and exiting");
            TaskExecutor.main(args);
            return;
        }

        logger.info("starting legacy task executor thread...");
        final Thread taskExecutorThread = new Thread(() -> {
            logger.info("delaying actual invocation of legacy task executor for 30 secs.....");
            Misc.sleep(30000);

            try {

                logger.info("actual invocation of legacy task executor");
                TaskExecutor.main(args);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        taskExecutorThread.setDaemon(true);
        taskExecutorThread.start();

        logger.info("starting legacy task executor...");
        SpringApplication.run(Application.class, args);
    }
}
