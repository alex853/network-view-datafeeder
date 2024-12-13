package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.io.Marker;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.misc.JavaTime;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.Position;
import net.simforge.networkview.core.report.ParsingLogics;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.compact.CompactifiedStorage;
import net.simforge.networkview.core.report.persistence.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SaveCompactified extends BaseTask {

    private static final String ARG_SINGLE = "single";
    private static final String ARG_STORAGE = "storage";
    private static final String ARG_KEEP_DAYS = "keep-days";

    private final Network network;
    private final String storageRoot;
    private final int keepDays;
    private final boolean singleRun;

    private final ReportJSONStorage storage;
    private final CompactifiedStorage compactifiedStorage;
    private final Marker marker;

    public SaveCompactified(final Properties properties) {
        super("SaveCmp-VATSIM-JSON");

        this.network = Network.VATSIM;
        this.storageRoot = properties.getProperty(ARG_STORAGE, ReportJSONStorage.DEFAULT_STORAGE_ROOT);
        this.keepDays = Math.max(Integer.parseInt(properties.getProperty(ARG_KEEP_DAYS)), 1);
        this.singleRun = Boolean.parseBoolean(properties.getProperty(ARG_SINGLE, "false"));

        this.storage = ReportJSONStorage.getStorage(storageRoot, network);
        this.compactifiedStorage = CompactifiedStorage.getStorage(storageRoot, network);
        this.marker = new Marker(getTaskName());

        setBaseSleepTime(1000);
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));

        RunningMarker.lock(getTaskName());

        logger.info("Network     : " + network);
        logger.info("Storage root: " + storageRoot);
        logger.info("Keep days   : " + keepDays);
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

            final String lastProcessedReport = marker.getString();

            final String nextReport;
            if (lastProcessedReport == null) {
                final LocalDateTime thresholdDt = JavaTime.nowUtc().minusDays(keepDays);

                String currReport = storage.getFirstReport();
                while (currReport != null) {
                    final LocalDateTime currDt = ReportUtils.fromTimestampJava(currReport);
                    if (currDt.isAfter(thresholdDt)) {
                        break;
                    }
                    currReport = storage.getNextReport(currReport);
                }

                nextReport = currReport;
                if (nextReport == null) {
                    logger.info("Still no first report found");
                }
            } else {
                nextReport = storage.getNextReport(lastProcessedReport);
            }

            if (nextReport == null) {
                return;
            }

            try {
                processReport(nextReport);

                marker.setString(nextReport);

                setNextSleepTime(100L); // small interval to catch up all remaining reports
            } catch (final Exception e) {
                logger.error("Error on report " + nextReport, e);

                logger.warn("Long sleep due to exception");
                setNextSleepTime(300000L); // 5 mins after exception
            }

        } catch (final IOException e) {
            logger.error("I/O exception happened", e);
            throw new RuntimeException("I/O exception happened", e);
        } finally {
            BM.stop();
        }
    }

    private void processReport(final String report) throws IOException {
        BM.start("processReport");
        try {

            logger.debug(ReportUtils.log(report) + " - Parsing started...");

            ReportJSONFile reportFile = storage.getReportFile(report);
            logger.debug(ReportUtils.log(report) + " -       Data splitted");

            List<ReportJSONFile.ClientInfo> pilotInfos = reportFile.getPilotInfos();
            logger.debug(ReportUtils.log(report) + " -       Data parsed");

            final Report _report = new Report();
            _report.setId(System.currentTimeMillis() / 1000);
            _report.setReport(report);

            savePilotPositions(_report, pilotInfos);

            logger.info(ReportUtils.log(report) + " -       Compactified report file saved");

        } finally {
            BM.stop();
        }
    }

    private void savePilotPositions(final Report report, final List<ReportJSONFile.ClientInfo> pilotInfos) {
        BM.start("savePilotPositions");
        try {
            final List<Position> positions = new ArrayList<>();

            for (final ReportJSONFile.ClientInfo pilotInfo : pilotInfos) {
                try {
                    final ReportPilotPosition p = new ReportPilotPosition();
                    p.setReport(report);
                    p.setPilotNumber(pilotInfo.getCid());
                    p.setCallsign(pilotInfo.getCallsign());
                    p.setLatitude(pilotInfo.getLatitude());
                    p.setLongitude(pilotInfo.getLongitude());
                    p.setAltitude(pilotInfo.getAltitude());
                    p.setGroundspeed(pilotInfo.getGroundspeed());
                    p.setHeading(pilotInfo.getHeading());
                    p.setFpAircraft(pilotInfo.getPlannedAircraft());
                    p.setFpOrigin(pilotInfo.getPlannedDepAirport());
                    p.setFpDestination(pilotInfo.getPlannedDestAirport());
                    p.setParsedRegNo(ParsingLogics.parseRegNo(p,
                            pilotInfo.getPlannedRemarks() != null
                            ? pilotInfo.getPlannedRemarks().trim()
                            : null));
                    p.setQnhMb(pilotInfo.getQnhMb());
                    p.setOnGround(pilotInfo.isOnGround());

                    final Position pp = Position.create(p);
                    positions.add(pp);
                } catch (final Exception e) {
                    final String msg = "Error on parsing data for PID " + pilotInfo.getCid();
                    logger.error(msg, e);
                    throw new RuntimeException(msg, e);
                }
            }

            logger.debug(ReportUtils.log(report) + " -       Pilot positions inserted");

            compactifiedStorage.savePositions(report.getReport(), positions);
        } finally {
            BM.stop();
        }
    }
}
