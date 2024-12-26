package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.hibernate.HibernateUtils;
import net.simforge.commons.io.Marker;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.misc.Misc;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ParsingLogics;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.persistence.*;
import net.simforge.networkview.datafeeder.CacheHelper;
import net.simforge.networkview.datafeeder.DatafeederTasks;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.Expirations;
import org.hibernate.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Parse extends BaseTask {

    private static final String ARG_SINGLE = "single";
    private static final String ARG_STORAGE = "storage";

    private Network network;
    private String storageRoot = ReportJSONStorage.DEFAULT_STORAGE_ROOT;
    private ReportJSONStorage storage;
    private boolean singleRun = false;
    @SuppressWarnings("FieldCanBeLocal")
    private Marker marker;
    private ReportSessionManager reportSessionManager;

    private CacheManager cacheManager;
    private Cache<String, ReportPilotFpRemarks> fpRemarksCache;

    public Parse(Properties properties) {
        super("Parse-VATSIM-JSON");
        init(properties);
    }

    private void init(Properties properties) {
        network = Network.VATSIM;

        storageRoot = properties.getProperty(ARG_STORAGE, storageRoot);

        singleRun = Boolean.parseBoolean(properties.getProperty(ARG_SINGLE, Boolean.toString(singleRun)));

        cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        fpRemarksCache = cacheManager.createCache("fpRemarksCache-" + network,
                CacheConfigurationBuilder
                        .newCacheConfigurationBuilder(
                                String.class,
                                ReportPilotFpRemarks.class,
                                ResourcePoolsBuilder.heap(10000))
                        .withExpiry(Expirations.timeToIdleExpiration(org.ehcache.expiry.Duration.of(10, TimeUnit.MINUTES)))
                        .build());

        setBaseSleepTime(1000);
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));
//        BM.setLoggingPeriod(TimeUnit.MINUTES.toMillis(5));

        RunningMarker.lock(getTaskName());

        logger.info("Network     : " + network);
        logger.info("Storage root: " + storageRoot);
        logger.info("Single run  : " + singleRun);


        storage = ReportJSONStorage.getStorage(storageRoot, network);
        marker = new Marker(getTaskName());
        reportSessionManager = DatafeederTasks.getSessionManager();

        setBaseSleepTime(1000);
    }

    @Override
    protected void shutdown() {
        super.shutdown();

        cacheManager.close();
    }

    @Override
    protected void process() {
        BM.start("Parse.process");
        try {

            String lastProcessedReport = marker.getString();

            String nextReport;
            if (lastProcessedReport == null) {
                nextReport = storage.getFirstReport();
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
            } catch (Exception e) {
                logger.error("Error on report " + nextReport, e);

                logger.warn("Long sleep due to exception");
                setNextSleepTime(300000L); // 5 mins after exception
            }

        } catch (IOException e) {
            logger.error("I/O exception happened", e);
            throw new RuntimeException("I/O exception happened", e);
        } finally {
            BM.stop();
        }
    }

    private void processReport(String report)
            throws IOException {
        BM.start("Parse.processReport");
        try (Session session = reportSessionManager.getSession(network)) {

            logger.debug(ReportUtils.log(report) + " - Parsing started...");

            ReportJSONFile reportFile = storage.loadReport(report);
            logger.debug(ReportUtils.log(report) + " -       Data splitted");

            List<ReportJSONFile.ClientInfo> pilotInfos = reportFile.getPilotInfos();
            List<ReportJSONFile.LogEntry> logEntries = new ArrayList<>(reportFile.getLog());
            logger.debug(ReportUtils.log(report) + " -       Data parsed");


            Report _report = ReportOps.loadReport(session, report);

            if (_report == null) {
                _report = new Report();
                _report.setReport(report);
                _report.setClients(pilotInfos.size());
                _report.setPilots(pilotInfos.size());
                _report.setHasLogs(!logEntries.isEmpty());
                _report.setParsed(false);

                HibernateUtils.saveAndCommit(session, "#createReport", _report);

                logger.debug(ReportUtils.log(report) + " -       Report record inserted");
            } else {
                logger.debug(ReportUtils.log(report) + " -       Report already exists ==> Only absent records will be added");
            }

            saveLogEntries(session, _report, logEntries);

            savePilotPositions(session, _report, pilotInfos);

            markReportParsed(session, _report);

            logger.info(ReportUtils.log(report) + " - \t\t\t\tStats | remarks " + CacheHelper.getEstimatedCacheSize(fpRemarksCache));

        } finally {
            BM.stop();
        }
    }

    private void saveLogEntries(Session session, Report _report, List<ReportJSONFile.LogEntry> logEntries) {
        BM.start("saveLogEntries");
        try {
            List<ReportLogEntry> existingLogEntries = loadExistingLogEntries(session, _report);

            for (ReportJSONFile.LogEntry logEntry : logEntries) {
                boolean logEntryExists = false;
                for (ReportLogEntry existingLogEntry : existingLogEntries) {
                    logEntryExists = Misc.equal(logEntry.getSection(), existingLogEntry.getSection())
                            && Misc.equal(logEntry.getObject(), existingLogEntry.getObject())
                            && Misc.equal(logEntry.getMsg(), existingLogEntry.getMessage())
                            && Misc.equal(logEntry.getValue(), existingLogEntry.getValue());
                    if (logEntryExists) {
                        break;
                    }
                }

                if (logEntryExists) {
                    continue;
                }

                logger.debug(ReportUtils.log(_report) + " -         LogEntry - S: " + logEntry.getSection() + "; O: " + logEntry.getObject() + "; M: " + logEntry.getMsg() + "; V: " + logEntry.getValue());
                ReportLogEntry l = new ReportLogEntry();
                l.setReport(_report);
                l.setSection(logEntry.getSection());
                l.setObject(logEntry.getObject());
                l.setMessage(logEntry.getMsg());
                l.setValue(logEntry.getValue());

                HibernateUtils.saveAndCommit(session, "#createLogEntry", l);

                existingLogEntries.add(l);
            }

            logger.debug(ReportUtils.log(_report) + " -       LogEntries added");
        } finally {
            BM.stop();
        }
    }

    private void savePilotPositions(Session session, Report _report, List<ReportJSONFile.ClientInfo> pilotInfos) {
        BM.start("savePilotPositions");
        try {
            List<ReportPilotPosition> existingPositions = ReportOps.loadPilotPositions(session, _report);

            for (ReportJSONFile.ClientInfo pilotInfo : pilotInfos) {
                try {
                    int pilotNumber = pilotInfo.getCid();
                    boolean positionExists = false;
                    for (ReportPilotPosition existingPosition : existingPositions) {
                        if (pilotNumber == existingPosition.getPilotNumber()) {
                            positionExists = true;
                            break;
                        }
                    }

                    if (positionExists) {
                        continue;
                    }

                    String fpRemarksStr = pilotInfo.getPlannedRemarks();
                    fpRemarksStr = fpRemarksStr != null ? fpRemarksStr.trim() : null;

                    ReportPilotFpRemarks fpRemarks = getFpRemarks(session, fpRemarksStr);

                    ReportPilotPosition p = new ReportPilotPosition();
                    p.setReport(_report);
                    p.setPilotNumber(pilotNumber);
                    p.setCallsign(pilotInfo.getCallsign());
                    p.setLatitude(pilotInfo.getLatitude());
                    p.setLongitude(pilotInfo.getLongitude());
                    p.setAltitude(pilotInfo.getAltitude());
                    p.setGroundspeed(pilotInfo.getGroundspeed());
                    p.setHeading(pilotInfo.getHeading());
                    p.setFpAircraft(pilotInfo.getPlannedAircraft());
                    p.setFpOrigin(pilotInfo.getPlannedDepAirport());
                    p.setFpDestination(pilotInfo.getPlannedDestAirport());
                    p.setFpRemarks(fpRemarks);
                    p.setParsedRegNo(ParsingLogics.parseRegNo(p, fpRemarksStr));
                    p.setQnhMb(pilotInfo.getQnhMb());
                    p.setOnGround(pilotInfo.isOnGround());

                    HibernateUtils.saveAndCommit(session, "#createPosition", p);

                    existingPositions.add(p);
                } catch (Exception e) {
                    String msg = "Error on saving data for PID " + pilotInfo.getCid();
                    logger.error(msg, e);
                    throw new RuntimeException(msg, e);
                }
            }

            logger.debug(ReportUtils.log(_report) + " -       Pilot positions inserted");
        } finally {
            BM.stop();
        }
    }

    private ReportPilotFpRemarks getFpRemarks(Session session, String fpRemarksStr) {
        BM.start("getFpRemarks");
        try {
            if (fpRemarksStr == null || fpRemarksStr.length() == 0) {
                // BM tag
                BM.start("#empty");
                BM.stop();

                return null;
            }

            ReportPilotFpRemarks fpRemarks = fpRemarksCache.get(fpRemarksStr);
            if (fpRemarks != null) {
                // BM tag
                BM.start("#cache-hit");
                BM.stop();

                return fpRemarks;
            }

            fpRemarks = ReportOps.loadFpRemarks(session, fpRemarksStr);
            if (fpRemarks != null) {
                // BM tag
                BM.start("#loaded-from-db");
                BM.stop();

                fpRemarksCache.put(fpRemarksStr, fpRemarks);
                return fpRemarks;
            }

            fpRemarks = new ReportPilotFpRemarks();
            fpRemarks.setRemarks(fpRemarksStr);

            HibernateUtils.saveAndCommit(session, "#save", fpRemarks);

            fpRemarksCache.put(fpRemarksStr, fpRemarks);

            return fpRemarks;
        } finally {
            BM.stop();
        }
    }

    private void markReportParsed(Session session, Report _report) {
        BM.start("markReportParsed");
        try {
            _report.setParsed(true);

            HibernateUtils.saveAndCommit(session, _report);

            logger.info(ReportUtils.log(_report) + " - Parsed");
        } finally {
            BM.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private List<ReportLogEntry> loadExistingLogEntries(Session session, Report _report) {
        BM.start("loadExistingLogEntries");
        try {
            return session
                    .createQuery("select l from ReportLogEntry l where l.report = :report")
                    .setEntity("report", _report)
                    .list();
        } finally {
            BM.stop();
        }
    }

}
