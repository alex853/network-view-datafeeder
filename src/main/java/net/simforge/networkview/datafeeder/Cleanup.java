package net.simforge.networkview.datafeeder;

import net.simforge.commons.hibernate.HibernateUtils;
import net.simforge.commons.io.Marker;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.misc.JavaTime;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.persistence.Report;
import net.simforge.networkview.core.report.persistence.ReportOps;
import net.simforge.networkview.core.report.persistence.ReportSessionManager;
import org.hibernate.Session;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Deprecated
public class Cleanup extends BaseTask {
    private static final String ARG_NETWORK = "network";
    private static final String ARG_KEEP_DAYS = "keep-days";

    private ReportSessionManager reportSessionManager;
    private Network network;
    @SuppressWarnings("FieldCanBeLocal")
    private long keepDays = 30;
    private Marker archivedReportMarker;

    @SuppressWarnings("unused")
    public Cleanup(Properties properties) {
        this(DatafeederTasks.getSessionManager(), Network.valueOf(properties.getProperty(ARG_NETWORK)), properties);
    }

    @SuppressWarnings("WeakerAccess")
    public Cleanup(ReportSessionManager reportSessionManager, Network network, Properties properties) {
        super("Cleanup-" + network);
        this.reportSessionManager = reportSessionManager;
        this.network = network;

        try {
            keepDays = Integer.parseInt(properties.getProperty(ARG_KEEP_DAYS));
        } catch (Exception e) {
            //noop
        }

        if (keepDays <= 0) {
            keepDays = 1;
        }
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));

        RunningMarker.lock(getTaskName());

        logger.info("Network        : " + network);
        logger.info("Keep days      : " + keepDays);

        archivedReportMarker = new Marker("Archive-" + network);
    }

    @Override
    protected void process() {
        BM.start("Cleanup.process");
        try (Session liveSession = reportSessionManager.getSession(network)) {
            Report report = ReportOps.loadFirstReport(liveSession);
            if (report == null) {
                logger.debug("No reports found");
                return; // standard sleep time
            }

            LocalDateTime reportDt = ReportUtils.fromTimestampJava(report.getReport());
            logger.debug(ReportUtils.log(report) + " - Cleanup started");

            String lastProcessedReport = archivedReportMarker.getString();
            if (lastProcessedReport == null) {
                logger.warn(ReportUtils.log(report) + " -     Archived report marker is empty");
                return; // standard sleep time
            }

            LocalDateTime lastProcessedReportDt = ReportUtils.fromTimestampJava(lastProcessedReport);
            LocalDateTime threshold = lastProcessedReportDt.minusDays(keepDays);
            logger.debug(ReportUtils.log(report) + " -     Threshold is {}", JavaTime.yMdHms.format(threshold));

            if (reportDt.isAfter(threshold)) {
                logger.debug(ReportUtils.log(report) + " -     Report is after threshold");
                return; // standard sleep time
            }

            removeReport(liveSession, report);

            logger.info(ReportUtils.log(report) + " - Cleanup complete");

            setNextSleepTime(1000); // short sleep time
        } finally {
            BM.stop();
        }
    }

    private void removeReport(Session liveSession, Report report) {
        BM.start("Cleanup.removeReport");
        try {
            HibernateUtils.transaction(liveSession, () -> {
                liveSession
                        .createQuery("delete from ReportPilotPosition where report = :report")
                        .setEntity("report", report)
                        .executeUpdate();

                liveSession
                        .createQuery("delete from ReportLogEntry where report = :report")
                        .setEntity("report", report)
                        .executeUpdate();

                liveSession.delete(report);
            });
        } finally {
            BM.stop();
        }
    }
}
