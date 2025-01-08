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
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class UploadArchive extends BaseTask {

    private static final String ARG_SINGLE = "single";

    private static final String yearPattern = "\\d{4}";
    private static final String monthPattern = "\\d{4}-\\d{2}";
    private static final String dateArchivePattern = "\\d{4}-\\d{2}-\\d{2}.zip";

    private final Network network = Network.VATSIM;
    private final String storageRoot;
    private final boolean singleRun;

    public UploadArchive(final Properties properties) {
        super("UplArch-VATSIM-JSON");

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

            File dateArchiveFile = null;

            final File[] years = listFolders(root, yearPattern);
            for (final File year : years) {
                if (dateArchiveFile != null) {
                    break;
                }

                final File[] months = listFolders(year, monthPattern);
                for (final File month : months) {
                    if (dateArchiveFile != null) {
                        break;
                    }

                    final File[] files = listFiles(month);
                    final List<File> archiveFiles = Arrays.stream(files).filter(f -> f.getName().matches(dateArchivePattern)).collect(Collectors.toList());

                    if (archiveFiles.isEmpty()) {
                        logger.warn("Month folder {} - no archive files found", month.getName());
                        continue;
                    }

                    dateArchiveFile = archiveFiles.get(0);
                }
            }

            if (dateArchiveFile == null) {
                logger.warn("No date folder for archival found");
                return;
            }

            final String s3BucketName = "simforge-data-archive";
            final String s3PathTemplate = "network-tracker/vatsim/vatsim-{year}/{file}";
            final LocalDate dateOfFile = LocalDate.parse(dateArchiveFile.getName().substring(0, 10));
            final int year = dateOfFile.getYear();
            final String s3Path = s3PathTemplate
                    .replace("{year}", String.valueOf(year))
                    .replace("{file}", dateArchiveFile.getName());
            logger.info("Archive file {} found - will be uploaded to {}:{}", dateArchiveFile.getName(), s3BucketName, s3Path);

            if (Math.random() == 0.0000000000000000000001) {
                throw new IOException();
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
