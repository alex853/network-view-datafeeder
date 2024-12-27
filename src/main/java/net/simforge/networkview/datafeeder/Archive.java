package net.simforge.networkview.datafeeder;

import net.simforge.commons.hibernate.HibernateUtils;
import net.simforge.commons.io.Marker;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.commons.runtime.ThreadMonitor;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.Position;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.persistence.*;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.Expirations;
import org.hibernate.Session;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Deprecated
public class Archive extends BaseTask {
    private static final String ARG_NETWORK = "network";

    private static final int REPORT_EVERY_N_MINUTES = 10;

    private ReportSessionManager reportSessionManager;
    private Network network;
    private Marker reportMarker;

    private Map<Integer, PilotTrack> pilotTracks = new HashMap<>();

    private CacheManager cacheManager;
    private Cache<String, Report> archivedReportsCache;
    private Cache<String, ReportPilotFpRemarks> archivedFpRemarksCache;

    @SuppressWarnings("unused")
    public Archive(Properties properties) {
        this(DatafeederTasks.getSessionManager(), Network.valueOf(properties.getProperty(ARG_NETWORK)));
    }

    @SuppressWarnings("WeakerAccess")
    public Archive(ReportSessionManager reportSessionManager, Network network) {
        super("Archive-" + network);
        this.reportSessionManager = reportSessionManager;
        this.network = network;
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));
//        BM.setLoggingPeriod(TimeUnit.MINUTES.toMillis(5));

        RunningMarker.lock(getTaskName());

        reportMarker = new Marker(getTaskName());

        cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        archivedReportsCache = cacheManager.createCache("archivedReportsCache-" + network,
                CacheConfigurationBuilder
                        .newCacheConfigurationBuilder(
                                String.class,
                                Report.class,
                                ResourcePoolsBuilder.heap(1000))
                        .withExpiry(Expirations.timeToIdleExpiration(org.ehcache.expiry.Duration.of(10, TimeUnit.MINUTES)))
                        .build());
        archivedFpRemarksCache = cacheManager.createCache("archivedFpRemarksCache-" + network,
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
    protected void shutdown() {
        super.shutdown();

        cacheManager.close();
    }

    @Override
    protected void process() {
        BM.start("Archive.process");
        try (Session liveSession = reportSessionManager.getSession(network)) {
            Report report;

            String lastProcessedReport = reportMarker.getString();
            if (lastProcessedReport == null) {
                report = ReportOps.loadFirstReport(liveSession);
            } else {
                report = ReportOps.loadNextReport(liveSession, lastProcessedReport);
            }

            if (report == null) {
                return; // standard sleep time
            }

            logger.debug(ReportUtils.log(report) + " - Archiving...");

            try (Session archiveSession = reportSessionManager.getSession(network, report.getReport())) {
                createArchivedReport(archiveSession, report);
            }

            List<ReportPilotPosition> currentPositions = ReportOps.loadPilotPositions(liveSession, report);
            logger.debug(ReportUtils.log(report) + " -     loaded {} positions", currentPositions.size());

            Map<Integer, ReportPilotPosition> pilotNumberToCurrentPosition = currentPositions
                    .stream()
                    .collect(Collectors.toMap(ReportPilotPosition::getPilotNumber, Function.identity()));

            Queue<ProcessingData> queue = new LinkedList<>();
            for (PilotTrack pilotTrack : pilotTracks.values()) {
                ReportPilotPosition currentPosition = pilotNumberToCurrentPosition.remove(pilotTrack.getPilotNumber());
                queue.add(new ProcessingData(pilotTrack.getPilotNumber(), pilotTrack, currentPosition));
            }

            for (ReportPilotPosition currentPosition : pilotNumberToCurrentPosition.values()) {
                queue.add(new ProcessingData(currentPosition.getPilotNumber(), null, currentPosition));
            }

            long lastPrintStatusTs = System.currentTimeMillis();
            int counter = 0;
            int queueSize = queue.size();

            while (!queue.isEmpty()) {
                ProcessingData processingData = queue.poll();
                processPilot(liveSession, report, processingData);

                ThreadMonitor.alive();
                counter++;
                long now = System.currentTimeMillis();
                if (now - lastPrintStatusTs > 10000L) {
                    logger.info(ReportUtils.log(report) + " -         " + counter + " of " + queueSize + " done");
                    lastPrintStatusTs = now;
                }
            }

            logger.info(ReportUtils.log(report) + " - Archived");

            printStats(report);

            reportMarker.setString(report.getReport());

            setNextSleepTime(1000); // short sleep time
        } finally {
            BM.stop();
        }
    }

    private static class ProcessingData {
        private int pilotNumber;
        private PilotTrack pilotTrack;
        private ReportPilotPosition currentPosition;

        ProcessingData(int pilotNumber, PilotTrack pilotTrack, ReportPilotPosition currentPosition) {
            this.pilotNumber = pilotNumber;
            this.pilotTrack = pilotTrack;
            this.currentPosition = currentPosition;
        }

        int getPilotNumber() {
            return pilotNumber;
        }

        PilotTrack getPilotTrack() {
            return pilotTrack;
        }

        ReportPilotPosition getCurrentPosition() {
            return currentPosition;
        }
    }

    private void processPilot(Session liveSession, Report report, ProcessingData processingData) {
        BM.start("processPilot");
        try {
            int pilotNumber = processingData.getPilotNumber();
            PilotTrack pilotTrack = processingData.getPilotTrack();
            ReportPilotPosition currentPosition = processingData.getCurrentPosition();

            if (pilotTrack == null) {
                List<ReportPilotPosition> previousReportPositions = loadPreviousLiveReports(liveSession, report, pilotNumber);

                pilotTrack = new PilotTrack(currentPosition.getPilotNumber());

                for (ReportPilotPosition position : previousReportPositions) {
                    pilotTrack.addPosition(position);
                }

                pilotTracks.put(pilotNumber, pilotTrack);
            }

            if (currentPosition != null) { // there is an ability to add support for going offline / going online
                pilotTrack.addPosition(currentPosition);
            }

            savePositionsToArchive(pilotTrack);

            cleanupExcessivePositions(pilotTrack);

            // dropping tracks without recent positions
            PositionInfo last = pilotTrack.getLastIn(PositionStatus.Unknown, PositionStatus.Excessive, PositionStatus.PositionReport, PositionStatus.TakeoffLanding);
            //noinspection ConstantConditions
            Duration difference = Duration.between(last.getPosition().getReportInfo().getDt(), ReportUtils.fromTimestampJava(report.getReport()));
            long differenceMillis = difference.toMillis();

            if (differenceMillis >= TimeUnit.MINUTES.toMillis(90)) {
                pilotTracks.remove(pilotNumber);
            }
        } finally {
            BM.stop();
        }
    }

    private Report getArchivedReport(Session archiveSession, String report) {
        BM.start("getArchivedReport");
        try {
            Report archivedReport = archivedReportsCache.get(report);
            if (archivedReport != null) {
                // BM tag
                BM.start("#cache-hit");
                BM.stop();

                return archivedReport;
            }

            archivedReport = ReportOps.loadParsedReport(archiveSession, report);
            if (archivedReport != null) {
                // BM tag
                BM.start("#loaded-from-db");
                BM.stop();

                archivedReportsCache.put(report, archivedReport);
            }

            return archivedReport;
        } finally {
            BM.stop();
        }
    }

    private void createArchivedReport(Session archiveSession, Report report) {
        BM.start("createArchivedReport");
        try {
            Report archivedReport = getArchivedReport(archiveSession, report.getReport());
            if (archivedReport != null) {
                return;
            }

            archivedReport = copy(report);
            archivedReport.setId(null);
            archivedReport.setVersion(null);

            HibernateUtils.saveAndCommit(archiveSession, "#save", archivedReport);

            archivedReportsCache.put(report.getReport(), archivedReport);
        } finally {
            BM.stop();
        }
    }

    private ReportPilotFpRemarks getArchivedFpRemarks(Session archiveSession, int reportYear, ReportPilotFpRemarks fpRemarks) {
        BM.start("getArchivedFpRemarks");
        try {
            ReportPilotFpRemarks archivedFpRemarks = archivedFpRemarksCache.get(reportYear + fpRemarks.getRemarks());
            if (archivedFpRemarks != null) {
                // BM tag
                BM.start("#cache-hit");
                BM.stop();

                return archivedFpRemarks;
            }

            archivedFpRemarks = ReportOps.loadFpRemarks(archiveSession, fpRemarks.getRemarks());
            if (archivedFpRemarks != null) {
                // BM tag
                BM.start("#loaded-from-db");
                BM.stop();

                archivedFpRemarksCache.put(reportYear + archivedFpRemarks.getRemarks(), archivedFpRemarks);
                return archivedFpRemarks;
            }

            final ReportPilotFpRemarks archivedFpRemarksCopy = copy(fpRemarks);
            archivedFpRemarksCopy.setId(null);
            archivedFpRemarksCopy.setVersion(null);

            HibernateUtils.saveAndCommit(archiveSession, "#save", archivedFpRemarksCopy);

            archivedFpRemarksCache.put(reportYear + archivedFpRemarksCopy.getRemarks(), archivedFpRemarksCopy);
            return archivedFpRemarksCopy;
        } finally {
            BM.stop();
        }
    }

    private void savePositionsToArchive(PilotTrack pilotTrack) {
        BM.start("savePositionsToArchive");
        try {
            List<PositionInfo> positions = pilotTrack.getPositions();
            for (PositionInfo position : positions) {
                PositionStatus status = position.getStatus();

                if (!(status == PositionStatus.PositionReport || status == PositionStatus.TakeoffLanding)) {
                    continue;
                }

                if (position.hasArchivedCopy()) {
                    continue;
                }

                String report = position.getReportPilotPosition().getReport().getReport();
                try (Session archiveSession = reportSessionManager.getSession(network, report)) {
                    Report archivedReport = getArchivedReport(archiveSession, report);

                    ReportPilotPosition archivedPositionCopy = ReportOps.loadPilotPosition(archiveSession, archivedReport, pilotTrack.getPilotNumber());

                    if (archivedPositionCopy != null) {
                        position.setHasArchivedCopy();
                        continue;
                    }

                    archivedPositionCopy = copy(position.getReportPilotPosition());
                    archivedPositionCopy.setId(null);
                    archivedPositionCopy.setVersion(null);
                    archivedPositionCopy.setReport(archivedReport);

                    ReportPilotFpRemarks fpRemarks = position.getReportPilotPosition().getFpRemarks();
                    if (fpRemarks != null) {
                        int reportYear = ReportUtils.fromTimestampJava(report).getYear();
                        archivedPositionCopy.setFpRemarks(getArchivedFpRemarks(archiveSession, reportYear, fpRemarks));
                    } else {
                        archivedPositionCopy.setFpRemarks(null);
                    }

                    HibernateUtils.saveAndCommit(archiveSession, "#save", archivedPositionCopy);

                    position.setHasArchivedCopy();
                }
            }
        } finally {
            BM.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private List<ReportPilotPosition> loadPreviousLiveReports(Session session, Report currentReport, int pilotNumber) {
        BM.start("loadPreviousLiveReports");
        try {
            return session
                    .createQuery("from ReportPilotPosition " +
                            "where pilotNumber = :pilotNumber" +
                            "    and report.id between :fromReportId and :toReportId " +
                            "order by report.id asc")
                    .setInteger("pilotNumber", pilotNumber)
                    .setLong("fromReportId", currentReport.getId() - 180)
                    .setLong("toReportId", currentReport.getId() - 1)
                    .list();
        } finally {
            BM.stop();
        }
    }

    private void printStats(Report report) {
        int totalPositions = 0;

        for (PilotTrack pilotTrack : pilotTracks.values()) {
            totalPositions += pilotTrack.getPositions().size();
        }

        logger.info(ReportUtils.log(report) + " - \t\t\t\tStats | {} tracks, {} positions, avg {} per track, reports {}, remarks {}",
                pilotTracks.size(),
                totalPositions,
                String.format("%.1f", totalPositions / (double) (pilotTracks.size() > 0 ? pilotTracks.size() : 1)),
                CacheHelper.getEstimatedCacheSize(this.archivedReportsCache),
                CacheHelper.getEstimatedCacheSize(this.archivedFpRemarksCache)
        );
    }

    private void cleanupExcessivePositions(PilotTrack pilotTrack) {
        // to cut everything older than ???
        // however to remain at least 1 position in "confirmed" state - Waypoint, TakeoffLanding
        // so most of tracks will have a state like:
        //   Waypoint
        //   Unknown (as last position is always in Unknown state)

        PositionInfo lastSavedPosition = pilotTrack.getLastIn(PositionStatus.PositionReport, PositionStatus.TakeoffLanding);
        if (lastSavedPosition == null) {
            return;
        }

        List<PositionInfo> toRemove = new ArrayList<>();
        List<PositionInfo> positions = pilotTrack.getPositions();
        for (PositionInfo position : positions) {
            if (position == lastSavedPosition) {
                break;
            }

            toRemove.add(position);
        }

        pilotTrack.removePositions(toRemove);
    }

    @SuppressWarnings("unchecked")
    private static <T> T copy(T src) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(src);
            oos.close();

            byte[] bytes = bos.toByteArray();

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object o = ois.readObject();
            return (T) o;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class PilotTrack {
        private int pilotNumber;
        private List<PositionInfo> positions = new ArrayList<>();

        PilotTrack(int pilotNumber) {
            this.pilotNumber = pilotNumber;
        }

        void addPosition(ReportPilotPosition position) {
            positions.add(new PositionInfo(position));
            updateStatuses();
        }

        private void updateStatuses() {
            for (int i = 0; i < positions.size(); i++) {
                PositionInfo current = positions.get(i);

                if (current.getStatus() != PositionStatus.Unknown) {
                    continue;
                }

                if (i == 0) {
                    current.setStatus(PositionStatus.PositionReport);
                    continue;
                }

                PositionInfo previous = positions.get(i - 1);

                Position currentPP = current.getPosition();
                Position previousPP = previous.getPosition();

                if (currentPP.isOnGround() && !previousPP.isOnGround()
                        || !currentPP.isOnGround() && previousPP.isOnGround()) {
                    // we have landing or airborne, lets have archive it
                    current.setStatus(PositionStatus.TakeoffLanding);
                    previous.setStatus(PositionStatus.TakeoffLanding);
                    continue;
                }

                PositionInfo previousSaved = getLastIn(PositionStatus.TakeoffLanding, PositionStatus.PositionReport);
                if (previousSaved == null) {
                    throw new IllegalStateException("Could not find previous saved position");
                }
                Position previousSavedPP = previousSaved.getPosition();

                Duration difference = Duration.between(previousSavedPP.getReportInfo().getDt(), currentPP.getReportInfo().getDt());
                long differenceMillis = difference.toMillis();

                if (differenceMillis >= TimeUnit.SECONDS.toMillis(REPORT_EVERY_N_MINUTES * 60 - 30)) {
                    current.setStatus(PositionStatus.PositionReport);
                } else {
                    current.setStatus(PositionStatus.Excessive);
                }
            }
        }

        private PositionInfo getLastIn(PositionStatus... statuses) {
            for (int j = positions.size() - 1; j >= 0; j--) {
                PositionInfo positionInfo = positions.get(j);

                PositionStatus status = positionInfo.getStatus();
                for (PositionStatus eachStatus : statuses) {
                    if (status == eachStatus) {
                        return positionInfo;
                    }
                }
            }

            return null;
        }

        void removePositions(List<PositionInfo> positions) {
            this.positions.removeAll(positions);
        }

        List<PositionInfo> getPositions() {
            return positions;
        }

        int getPilotNumber() {
            return pilotNumber;
        }
    }

    private static class PositionInfo {
        private PositionStatus status = PositionStatus.Unknown;
        private ReportPilotPosition reportPilotPosition;
        private Position position;
        private boolean hasArchivedCopy;

        PositionInfo(ReportPilotPosition reportPilotPosition) {
            this.reportPilotPosition = reportPilotPosition;
            this.position = Position.create(reportPilotPosition);
        }

        ReportPilotPosition getReportPilotPosition() {
            return reportPilotPosition;
        }

        Position getPosition() {
            return position;
        }

        PositionStatus getStatus() {
            return status;
        }

        void setStatus(PositionStatus status) {
            this.status = status;
        }

        boolean hasArchivedCopy() {
            return hasArchivedCopy;
        }

        void setHasArchivedCopy() {
            this.hasArchivedCopy = true;
        }

        @Override
        public String toString() {
            return "Position " + position.getReportInfo().getDt() + ", " + (position.isOnGround() ? "On Ground" : "Flying") + ", " + status;
        }
    }

    enum PositionStatus {
        Unknown, TakeoffLanding, PositionReport, Excessive
    }
}
