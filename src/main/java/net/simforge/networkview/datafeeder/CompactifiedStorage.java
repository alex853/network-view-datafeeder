package net.simforge.networkview.datafeeder;

import net.simforge.commons.legacy.BM;
import net.simforge.networkview.core.CompactifiedPosition;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.Position;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.persistence.ReportPilotPosition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// todo ak separate file-writer-reader for specific version and tests on it
public class CompactifiedStorage {
    private static final int V1_HEAD = 1;
    private static final int V1_EOF = 0xffffffff;

    private final Path root;

    private CompactifiedStorage(final String rootPath, final Network network) {
        root = Paths.get(rootPath, network.name(), "/compactified");
    }

    public static CompactifiedStorage getStorage(final String storageRoot, final Network network) {
        return new CompactifiedStorage(storageRoot, network);
    }

    public void save(final String report, final List<ReportPilotPosition> positions) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        baos.write(V1_HEAD);
        baos.write(positions.size());

        positions.forEach(p -> {
            final Position position = Position.create(p);
            final CompactifiedPosition compactPosition = Position.compactify(position);
            try {
                baos.write(compactPosition.asBytes());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            baos.write(V1_EOF);
            baos.close();

            if (!Files.exists(root)) {
                Files.createDirectory(root);
            }
            Files.write(root.resolve(report),
                    baos.toByteArray(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (final IOException e) {
            throw new RuntimeException(e);
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
