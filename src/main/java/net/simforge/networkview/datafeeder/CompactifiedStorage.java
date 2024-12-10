package net.simforge.networkview.datafeeder;

import net.simforge.networkview.core.CompactifiedPosition;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.Position;
import net.simforge.networkview.core.report.persistence.ReportPilotPosition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;

// todo ak separate file-writer-reader for specific version and tests on it
public class CompactifiedStorage {
    private static final int V1_HEAD = 1;
    private static final int V1_EOF = 0xffffffff;

    private final Path root;

    private CompactifiedStorage(String rootPath, Network network) {
        root = Paths.get(rootPath, network.name(), "/compactified");
    }

    public static CompactifiedStorage getStorage(String storageRoot, Network network) {
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
            Files.write(root.resolve(report), baos.toByteArray(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
