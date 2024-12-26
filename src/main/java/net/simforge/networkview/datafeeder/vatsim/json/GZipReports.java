package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.io.IOHelper;
import net.simforge.commons.io.Marker;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class GZipReports extends BaseTask {

    private static final String ARG_SINGLE = "single";
    private static final String ARG_STORAGE = "storage";

    private final Network network = Network.VATSIM;
    private final String storageRoot;
    private final boolean singleRun;

    private final ReportJSONStorage storage;
    private final Marker compactifiedMarker = new Marker("SaveCmp-VATSIM-JSON");

    public GZipReports(final Properties properties) {
        super("GZipRep-VATSIM-JSON");

        this.storageRoot = properties.getProperty(ARG_STORAGE, ReportJSONStorage.DEFAULT_STORAGE_ROOT);
        this.singleRun = Boolean.parseBoolean(properties.getProperty(ARG_SINGLE, "false"));

        this.storage = ReportJSONStorage.getStorage(storageRoot, network);

        setBaseSleepTime(1000);
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));

        RunningMarker.lock(getTaskName());

        logger.info("Network     : " + network);
        logger.info("Storage root: " + storageRoot);
        logger.info("Single run  : " + singleRun);
    }

    @Override
    protected void shutdown() {
        super.shutdown();
    }

    @Override
    protected void process() {
        BM.start("process");
        try {

            final String report = storage.getFirstReport();
            if (report == null) {
                logger.warn("No report found");
                return;
            }

            final String lastCompactifiedReport = compactifiedMarker.getString();
            if (lastCompactifiedReport == null) {
                logger.warn("No compactified report found");
                return;
            }

            final LocalDateTime reportDt = ReportUtils.fromTimestampJava(report);
            final LocalDateTime lastCompactifiedReportDt = ReportUtils.fromTimestampJava(lastCompactifiedReport);

            final LocalDateTime thresholdDt = lastCompactifiedReportDt.minusHours(1);
            if (reportDt.isAfter(thresholdDt)) {
                logger.info("No reports for zipping at the moment");
                return;
            }

            final File reportFile = storage.getReportFile(report);
            final File gzippedReportFile = new File(reportFile.getParentFile(), report + ".json.gz");

            try (final FileInputStream fis = new FileInputStream(reportFile);
                 final FileOutputStream fos = new FileOutputStream(gzippedReportFile);
                 final GZIPOutputStream gos = new GZIPOutputStream(fos)) {
                IOHelper.copyStream(fis, gos);
            }

            if (!reportFile.delete()) {
                throw new IOException("unable to delete report file " + reportFile);
            }

            logger.info(ReportUtils.log(report) + " -       Report file gzipped");

            setNextSleepTime(100L); // small interval to catch up all remaining reports

        } catch (final IOException e) {
            logger.error("I/O exception happened", e);
            throw new RuntimeException("I/O exception happened", e);
        } finally {
            BM.stop();
        }
    }
}
