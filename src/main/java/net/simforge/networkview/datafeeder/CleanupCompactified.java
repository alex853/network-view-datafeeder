package net.simforge.networkview.datafeeder;

import net.simforge.commons.legacy.BM;
import net.simforge.commons.misc.JavaTime;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.compact.CompactifiedStorage;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CleanupCompactified extends BaseTask {
    private static final String ARG_NETWORK = "network";
    private static final String ARG_STORAGE = "storage";
    private static final String ARG_KEEP_DAYS = "keep-days";

    private final Network network;
    private final int keepDays;
    private final String storageRoot;

    private final CompactifiedStorage storage;

    @SuppressWarnings("unused")
    public CleanupCompactified(final Properties properties) {
        this(Network.valueOf(properties.getProperty(ARG_NETWORK)), properties);
    }

    @SuppressWarnings("WeakerAccess")
    public CleanupCompactified(final Network network, final Properties properties) {
        super("CleanCompact-" + network);
        this.network = network;
        this.keepDays = Math.max(Integer.parseInt(properties.getProperty(ARG_KEEP_DAYS)), 1);
        this.storageRoot = properties.getProperty(ARG_STORAGE, "./data");
        this.storage = CompactifiedStorage.getStorage(storageRoot, network);
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));

        RunningMarker.lock(getTaskName());

        logger.info("Network        : " + network);
        logger.info("Storage root   : " + storageRoot);
        logger.info("Keep days      : " + keepDays);
    }

    @Override
    protected void process() {
        BM.start("CleanupCompactified.process");
        try {
            final LocalDateTime thresholdDt = JavaTime.nowUtc().minusDays(keepDays);

            final String report = storage.getFirstReport();
            if (report == null) {
                logger.debug("No reports found");
                return; // standard sleep time
            }

            final LocalDateTime reportDt = ReportUtils.fromTimestampJava(report);
            if (reportDt.isAfter(thresholdDt)) {
                logger.debug(ReportUtils.log(report) + " -     Threshold is {}, nothing to cleanup", JavaTime.yMdHms.format(thresholdDt));
                return; // standard sleep time
            }

            removeReport(report);

            logger.info(ReportUtils.log(report) + " - Cleanup complete");

            setNextSleepTime(1000); // short sleep time
        } catch (IOException e) {
            logger.error("error on processing", e);
        } finally {
            BM.stop();
        }
    }

    private void removeReport(final String report) {
        BM.start("CleanupCompactified.removeReport");
        try {
            storage.removeReport(report);
        } catch (IOException e) {
            logger.error("error on removal", e);
        } finally {
            BM.stop();
        }
    }
}
