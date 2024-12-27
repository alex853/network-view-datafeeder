package net.simforge.networkview.datafeeder;

import net.simforge.commons.legacy.misc.Settings;
import net.simforge.commons.misc.JavaTime;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.core.report.compact.CompactifiedStorage;
import net.simforge.networkview.datafeeder.vatsim.json.ReportJSONStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;

@RestController
@RequestMapping("/")
public class StatusController {
    private static final Logger logger = LoggerFactory.getLogger(StatusController.class);

    @GetMapping("hello-world")
    public String getHelloWorld() {
        return "Hello, World!";
    }

    @GetMapping("vatsim-status")
    public ResponseEntity<VatsimStatusDto> getVatsimStatus() throws IOException {
        final String storageRoot = Settings.get(SettingNames.storageRoot);

        final ReportJSONStorage jsonStorage = ReportJSONStorage.getStorage(storageRoot, Network.VATSIM);
        final CompactifiedStorage compactifiedStorage = CompactifiedStorage.getStorage(storageRoot, Network.VATSIM);

        final LocalDateTime now = JavaTime.nowUtc();

        final String lastJsonReport = jsonStorage.getLastReport();
        final String lastCompactifiedReport = compactifiedStorage.getLastReport();

        final LocalDateTime lastJsonReportDt = ReportUtils.fromTimestampJava(lastJsonReport);
        final LocalDateTime lastCompactifiedReportDt = ReportUtils.fromTimestampJava(lastCompactifiedReport);

        final Duration jsonReportLag = Duration.between(lastJsonReportDt, now);
        final Duration compactifiedReportLag = Duration.between(lastCompactifiedReportDt, now);

        final boolean jsonReportLagOk = jsonReportLag.getSeconds() < 600;
        final boolean compactifiedReportLagOk = compactifiedReportLag.getSeconds() < 600;
        final boolean ok = jsonReportLagOk && compactifiedReportLagOk;

        if (ok) {
            return ResponseEntity.ok(new VatsimStatusDto(
                    "ok",
                    lastJsonReport,
                    lastCompactifiedReport));
        } else {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body(new VatsimStatusDto(
                            "fail",
                            lastJsonReport,
                            lastCompactifiedReport));
        }
    }

    @GetMapping("disk-status")
    public ResponseEntity<DiskStatusDto> getDiskStatus() {
        final File file = new File("/");

        final long freeSpaceBytes = file.getFreeSpace();
        final long freeSpaceMb = freeSpaceBytes / (1024 * 1024);

        final boolean ok = freeSpaceMb > 10000;

        if (ok) {
            return ResponseEntity.ok(new DiskStatusDto(
                    "ok",
                    freeSpaceMb));
        } else {
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body(new DiskStatusDto(
                            "fail",
                            freeSpaceMb));
        }
    }

    private static String bytesToGigabytes(long bytes) {
        double gigabytes = bytes / (1024.0 * 1024 * 1024);
        return String.format("%.2f", gigabytes);
    }

    private static class VatsimStatusDto {
        private final String status;
        private final String lastJsonReport;
        private final String lastCompactifiedReport;

        private VatsimStatusDto(final String status,
                               final String lastJsonReport,
                               final String lastCompactifiedReport) {
            this.status = status;
            this.lastJsonReport = lastJsonReport;
            this.lastCompactifiedReport = lastCompactifiedReport;
        }

        public String getStatus() {
            return status;
        }

        public String getLastJsonReport() {
            return lastJsonReport;
        }

        public String getLastCompactifiedReport() {
            return lastCompactifiedReport;
        }
    }

    private static class DiskStatusDto {
        private final String status;
        private final long freeSpaceMb;

        private DiskStatusDto(final String status, final long freeSpaceMb) {
            this.status = status;
            this.freeSpaceMb = freeSpaceMb;
        }

        public String getStatus() {
            return status;
        }

        public long getFreeSpaceMb() {
            return freeSpaceMb;
        }
    }
}
