package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.io.IOHelper;
import net.simforge.commons.legacy.BM;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReportJSONStorage {

    public static final String DEFAULT_STORAGE_ROOT = "../data";

    private static final DateTimeFormatter yyyy = DateTimeFormatter.ofPattern("yyyy");//.withZoneUTC();
    private static final DateTimeFormatter yyyyMM = DateTimeFormatter.ofPattern("yyyy-MM");//.withZoneUTC();
    private static final DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");//.withZoneUTC();

    private final File root;
    private final Network network;

    private ReportJSONStorage(final String rootPath, final Network network) {
        this.network = network;

        root = new File(rootPath + "/" + network.name());
        //noinspection ResultOfMethodCallIgnored
        root.mkdirs();
    }

    public static ReportJSONStorage getStorage(final String storageRoot, final Network network) {
        return new ReportJSONStorage(storageRoot, network);
    }

    public File getRoot() {
        return root;
    }

    public void saveReport(final String report, final String data) throws IOException {
        BM.start("ReportJSONStorage.saveReport");
        try {
            final File file = getReportFile(report);
            //noinspection ResultOfMethodCallIgnored
            file.getParentFile().mkdirs();
            IOHelper.saveFile(file, data);
        } finally {
            BM.stop();
        }
    }

    public String getFirstReport() throws IOException {
        BM.start("ReportJSONStorage.getFirstReport");
        try {
            final List<String> allReports = listAllReports();
            if (allReports.size() == 0) {
                return null;
            }
            return allReports.get(0);
        } finally {
            BM.stop();
        }
    }

    public String getNextReport(final String previousReport) throws IOException {
        BM.start("ReportJSONStorage.getNextReport");
        try {
            final List<String> allReports = listAllReports();
            if (allReports.size() == 0) {
                return null;
            }
            final int index = allReports.indexOf(previousReport);
            if (index == -1) {
                return null;
            }
            if (index == allReports.size() - 1) {
                return null;
            }
            return allReports.get(index + 1);
        } finally {
            BM.stop();
        }
    }

    public String getLastReport() throws IOException {
        BM.start("ReportJSONStorage.getLastReport");
        try {
            final List<String> allReports = listAllReports();
            if (allReports.size() == 0) {
                return null;
            }
            return allReports.get(allReports.size() - 1);
        } finally {
            BM.stop();
        }
    }

    public ReportJSONFile loadReport(final String report) throws IOException {
        BM.start("ReportJSONStorage.loadReport");
        try {
            final File file = getReportFile(report);
            final String content = IOHelper.loadFile(file);
            return new ReportJSONFile(network, content);
        } finally {
            BM.stop();
        }
    }

    public List<String> listAllReports() throws IOException {
        BM.start("ReportJSONStorage.listAllReports");
        try {
            final List<String> reports = new ArrayList<>();

            Files.walkFileTree(Paths.get(root.toURI()), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
                    if (!attrs.isRegularFile()) {
                        return FileVisitResult.CONTINUE;
                    }

                    String filename = file.getFileName().toString();
                    if (!filename.endsWith(".json")) {
                        return FileVisitResult.CONTINUE;
                    }

                    filename = filename.substring(0, filename.length() - ".json".length());

                    if (!ReportUtils.isTimestamp(filename)) {
                        return FileVisitResult.CONTINUE;
                    }

                    reports.add(filename);

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });

            Collections.sort(reports);

            return reports;
        } finally {
            BM.stop();
        }
    }

    public File getReportFile(final String report) {
        final String filename = reportToFullPath(report);
        return new File(root, filename);
    }

    private String reportToFullPath(final String report) {
        final LocalDateTime dateTime = ReportUtils.fromTimestampJava(report);
        return yyyy.format(dateTime) + "/" + yyyyMM.format(dateTime) + "/" + yyyyMMdd.format(dateTime) + "/" + report + ".json";
    }
}
