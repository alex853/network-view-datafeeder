package net.simforge.networkview.datafeeder.compact;

import net.simforge.commons.legacy.BM;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.Position;
import net.simforge.networkview.core.report.ReportUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompactifiedStorage {
    private final Path root;

    private CompactifiedStorage(final String rootPath, final Network network) {
        root = Paths.get(rootPath, network.name(), "compactified");

        try {
            if (!Files.exists(root)) {
                Files.createDirectory(root);
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to create root folder", e);
        }
    }

    public static CompactifiedStorage getStorage(final String storageRoot, final Network network) {
        return new CompactifiedStorage(storageRoot, network);
    }

    public void savePositions(final String report, final List<Position> positions) {
        final Path reportPath = root.resolve(report);

        try (final FileOutputStream fos = new FileOutputStream(reportPath.toFile())) {
            V1Ops.saveToStream(positions, fos);
        } catch (final IOException e) {
            throw new RuntimeException("unable to save compactified report data", e);
        }
    }

    public List<Position> loadPositions(final String report) {
        final Path reportPath = root.resolve(report);

        if (!Files.exists(reportPath)) {
            throw new IllegalArgumentException("unable to find report " + report);
        }

        try (final FileInputStream fis = new FileInputStream(reportPath.toFile())) {
            return V1Ops.loadFromStream(fis);
        } catch (final IOException e) {
            throw new RuntimeException("unable to load compactified report data", e);
        }
    }

    public String getFirstReport() throws IOException {
        final List<String> allReports = listAllReportFiles();
        if (allReports.size() == 0) {
            return null;
        }
        return allReports.get(0);
    }

    public String getNextReport(String previousReport) throws IOException {
        final List<String> allReports = listAllReportFiles();
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
    }

    public String getLastReport() throws IOException {
        final List<String> allReports = listAllReportFiles();
        if (allReports.size() == 0) {
            return null;
        }
        return allReports.get(allReports.size() - 1);
    }

    private List<String> listAllReportFiles() throws IOException {
        BM.start("CompactifiedStorage.listAllReportFiles");
        try {
            final List<String> reports = new ArrayList<>();

            Files.walkFileTree(root, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
                    if (!attrs.isRegularFile()) {
                        return FileVisitResult.CONTINUE;
                    }

                    final String filename = file.getFileName().toString();
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

    public void removeReport(final String report) throws IOException {
        Files.deleteIfExists(root.resolve(report));
    }
}
