package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.io.IOHelper;
import net.simforge.networkview.core.Network;
import org.junit.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReportJSONFileTest {
    @Test
    public void load() throws IOException {
        String data = IOHelper.readInputStream(ReportJSONFile.class.getResourceAsStream("/20210512000120.json"));
        ReportJSONFile report = new ReportJSONFile(Network.VATSIM, data);

        assertEquals("20210512000120", report.getUpdate());
        assertEquals(577, report.getPilotInfos().size());
    }

    @Test
    public void load_QNHoutOfRange() throws IOException {
        String data = IOHelper.readInputStream(ReportJSONFile.class.getResourceAsStream("/20210524014805.json"));
        ReportJSONFile report = new ReportJSONFile(Network.VATSIM, data);

        assertEquals("20210524014805", report.getUpdate());

        ReportJSONFile.ClientInfo pilotInfo = report.getPilotInfos().stream().filter(pi -> pi.getCid() == 1479856).findFirst().get();
        assertEquals(0, pilotInfo.getQnhMb().intValue());
    }
}