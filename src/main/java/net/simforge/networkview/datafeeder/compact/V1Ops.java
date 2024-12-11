package net.simforge.networkview.datafeeder.compact;

import net.simforge.networkview.core.CompactifiedPosition;
import net.simforge.networkview.core.Position;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class V1Ops {
    private static final int V1_HEAD = 1;
    private static final int V1_EOF = 0xffffffff;

    public static void saveToStream(final List<Position> positions,
                                    final OutputStream os) throws IOException {
        final DataOutputStream out = new DataOutputStream(os);
        out.writeInt(V1_HEAD);
        out.writeInt(positions.size());

        for (final Position position : positions) {
            final CompactifiedPosition compactPosition = Position.compactify(position);
            out.write(compactPosition.asBytes());
        }

        out.writeInt(V1_EOF);
        out.flush();
    }

    public static List<Position> loadFromStream(final InputStream is) throws IOException {
        final DataInputStream in = new DataInputStream(is);

        final int head = in.readInt();
        if (head != V1_HEAD) {
            throw new IllegalStateException("unknown report head");
        }

        final List<Position> result = new ArrayList<>();
        final int expectedCount = in.readInt();
        for (int i = 0; i < expectedCount; i++) {
            final byte[] bytes = new byte[CompactifiedPosition.TOTAL_LENGTH];
            final int actual = in.read(bytes, 0, bytes.length);
            if (actual != CompactifiedPosition.TOTAL_LENGTH) {
                throw new IllegalStateException("unable to read data correctly");
            }
            result.add(CompactifiedPosition.fromBytes(bytes));
        }

        final int eof = in.readInt();
        if (eof != V1_EOF) {
            throw new IllegalStateException("can't find eof");
        }

        if (expectedCount != result.size()) {
            throw new IllegalStateException("actual record count differs from expected");
        }

        return result;
    }
}
