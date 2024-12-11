package net.simforge.networkview.datafeeder.compact;

import net.simforge.networkview.core.Position;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class V1OpsTest {
    @Test
    public void test_load_save_load() throws IOException {
        final InputStream in = V1OpsTest.class.getResourceAsStream("/20241211205134");
        final List<Position> originalPositions = V1Ops.loadFromStream(in);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        V1Ops.saveToStream(originalPositions, baos);
        final byte[] bytes = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final List<Position> processedPositions = V1Ops.loadFromStream(bais);

        assertEquals(originalPositions.size(), processedPositions.size());
        for (int i = 0; i < originalPositions.size(); i++) {
            final Position op = originalPositions.get(i);
            final Position pp = processedPositions.get(i);

            assertEquals(op.getCallsign(), pp.getCallsign());
            assertEquals(op.getActualAltitude(), pp.getActualAltitude());
            assertTrue(op.getCoords().isSame(pp.getCoords()));
        }
    }
}