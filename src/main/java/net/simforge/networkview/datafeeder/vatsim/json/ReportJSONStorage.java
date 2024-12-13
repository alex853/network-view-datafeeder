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

    private static DateTimeFormatter yyyy = DateTimeFormatter.ofPattern("yyyy");//.withZoneUTC();
    private static DateTimeFormatter yyyyMM = DateTimeFormatter.ofPattern("yyyy-MM");//.withZoneUTC();
    private static DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyy-MM-dd");//.withZoneUTC();

    private File root;
    private Network network;

    private ReportJSONStorage(String rootPath, Network network) {
        this.network = network;

        root = new File(rootPath + "/" + network.name());
        //noinspection ResultOfMethodCallIgnored
        root.mkdirs();
    }

    public static ReportJSONStorage getStorage(String storageRoot, Network network) {
        return new ReportJSONStorage(storageRoot, network);
    }

    public File getRoot() {
        return root;
    }

    public void saveReport(String report, String data) throws IOException {
        String filename = reportToFullPath(report);
        File file = new File(root, filename);
        //noinspection ResultOfMethodCallIgnored
        file.getParentFile().mkdirs();
        IOHelper.saveFile(file, data);
    }

    private String reportToFullPath(String report) {
        LocalDateTime dateTime = ReportUtils.fromTimestampJava(report);
        return yyyy.format(dateTime) + "/" + yyyyMM.format(dateTime) + "/" + yyyyMMdd.format(dateTime) + "/" + report + ".json";
    }

    public String getFirstReport() throws IOException {
        List<String> allReports = listAllReports();
        if (allReports.size() == 0) {
            return null;
        }
        return allReports.get(0);
    }

    public String getNextReport(String previousReport) throws IOException {
        List<String> allReports = listAllReports();
        if (allReports.size() == 0) {
            return null;
        }
        int index = allReports.indexOf(previousReport);
        if (index == -1) {
            return null;
        }
        if (index == allReports.size() - 1) {
            return null;
        }
        return allReports.get(index + 1);
    }

    public String getLastReport() throws IOException {
        List<String> allReports = listAllReports();
        if (allReports.size() == 0) {
            return null;
        }
        return allReports.get(allReports.size() - 1);
    }

    public ReportJSONFile getReportFile(String report) throws IOException {
        String filename = reportToFullPath(report);
        File file = new File(root, filename);
        String content = IOHelper.loadFile(file);
        //noinspection UnnecessaryLocalVariable
        ReportJSONFile reportFile = new ReportJSONFile(network, content);
        return reportFile;
    }

    public List<String> listAllReports() throws IOException {
        BM.start("ReportJSONStorage.listAllReportFiles");
        try {
            final List<String> reports = new ArrayList<>();

            Files.walkFileTree(Paths.get(root.toURI()), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
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
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });

            Collections.sort(reports);

            return reports;
        } finally {
            BM.stop();
        }
    }
}
