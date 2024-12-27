package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.io.IOHelper;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.legacy.misc.Settings;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.datafeeder.SettingNames;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DailyArchive extends BaseTask {

    private static final String ARG_SINGLE = "single";

    private static final String yearPattern = "\\d{4}";
    private static final String monthPattern = "\\d{4}-\\d{2}";
    private static final String datePattern = "\\d{4}-\\d{2}-\\d{2}";

    private final Network network = Network.VATSIM;
    private final String storageRoot;
    private final boolean singleRun;

    public DailyArchive(final Properties properties) {
        super("DayArch-VATSIM-JSON");

        this.storageRoot = Settings.get(SettingNames.storageRoot) != null ? Settings.get(SettingNames.storageRoot) : ReportJSONStorage.DEFAULT_STORAGE_ROOT;
        this.singleRun = Boolean.parseBoolean(properties.getProperty(ARG_SINGLE, "false"));

        setBaseSleepTime(3600000);
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));

        RunningMarker.lock(getTaskName());

        logger.info("Network     : " + network);
        logger.info("Storage root: " + storageRoot);
        logger.info("Single run  : " + singleRun);
    }

    @Override
    protected void shutdown() {
        super.shutdown();
    }

    @Override
    protected void process() {
        BM.start("process");
        try {

            final File root = new File(storageRoot + "/" + network.name());

            File dateFolderFile = null;
            List<File> filesToArchive = null;

            final File[] years = listFolders(root, yearPattern);
            for (final File year : years) {
                if (dateFolderFile != null) {
                    break;
                }

                final File[] months = listFolders(year, monthPattern);
                for (final File month : months) {
                    if (dateFolderFile != null) {
                        break;
                    }

                    final File[] dates = listFolders(month, datePattern);
                    for (final File date : dates) {
                        if (dateFolderFile != null) {
                            break;
                        }

                        final File[] files = listFiles(date);
                        final boolean anyNonGz = Arrays.stream(files).anyMatch(f -> !f.getName().endsWith(".gz"));
                        final List<File> gzReportFiles = Arrays.stream(files).filter(f -> f.getName().endsWith(".gz")).collect(Collectors.toList());

                        if (gzReportFiles.isEmpty()) {
                            logger.warn("Date folder {} - no gz files found", date.getName());
                            continue;
                        }

                        if (anyNonGz) {
                            logger.warn("Date folder {} - non-gz files presented", date.getName());
                            continue;
                        }

                        dateFolderFile = date;
                        filesToArchive = gzReportFiles;
                    }
                }
            }

            if (dateFolderFile == null) {
                logger.warn("No date folder for archival found");
                return;
            }

            logger.info("Date folder {} - starting archival of {} reports", dateFolderFile.getName(), filesToArchive.size());

            final File tempFile = new File(dateFolderFile.getParentFile(), System.currentTimeMillis() + ".zip");
            final File targetFile = new File(dateFolderFile.getAbsolutePath() + ".zip");

            if (targetFile.exists()) {
                logger.error("Date folder {} - target archival file EXISTS! terminating", dateFolderFile.getName());
                return;
            }

            long lastInfoTime = System.currentTimeMillis();
            try (final FileOutputStream fos = new FileOutputStream(tempFile);
                 final ZipOutputStream zos = new ZipOutputStream(fos)) {

                zos.setLevel(Deflater.BEST_COMPRESSION);

                int counter = 0;
                for (final File file : filesToArchive) {
                    try (final FileInputStream fis = new FileInputStream(file)) {
                        final ZipEntry zipEntry = new ZipEntry(file.getName());
                        zos.putNextEntry(zipEntry);
                        IOHelper.copyStream(fis, zos);
                        zos.closeEntry();
                    }

                    counter++;

                    if (System.currentTimeMillis() - lastInfoTime >= 10000) {
                        logger.info("Date folder {} - archived {} / {}", dateFolderFile.getName(), counter, filesToArchive.size());
                        lastInfoTime = System.currentTimeMillis();
                    }
                }
            }

            logger.info("Date folder {} - All archived", dateFolderFile.getName());

            final boolean renameResult = tempFile.renameTo(targetFile);

            if (!renameResult) {
                logger.error("Date folder {} - renaming FAILED! terminating", dateFolderFile.getName());
                return;
            }

            for (final File file : filesToArchive) {
                if (!file.delete()) {
                    logger.error("Date folder {} - COULD NOT DELETE {}", dateFolderFile.getName(), file.getName());
                }
            }

            if (!dateFolderFile.delete()) {
                logger.error("Date folder {} - COULD NOT DELETE DATE FOLDER", dateFolderFile.getName());
            } else {
                logger.info("Date folder {} - Cleanup after archival completed", dateFolderFile.getName());
            }

        } catch (final IOException e) {
            logger.error("I/O exception happened", e);
            throw new RuntimeException("I/O exception happened", e);
        } finally {
            BM.stop();
        }
    }

    private File[] listFolders(final File parent, final String pattern) {
        final File[] files = parent.listFiles(f -> f.isDirectory() && f.getName().matches(pattern));
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    private File[] listFiles(final File parent) {
        final File[] files = parent.listFiles(File::isFile);
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }
}
