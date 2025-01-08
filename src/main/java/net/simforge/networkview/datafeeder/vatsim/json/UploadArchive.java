package net.simforge.networkview.datafeeder.vatsim.json;

import net.simforge.commons.legacy.BM;
import net.simforge.commons.legacy.misc.Settings;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.datafeeder.SettingNames;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            logger.info("Archive file {} - found - will be uploaded to {}:{}", dateArchiveFile.getName(), s3BucketName, s3Path);

            final Region region = Region.US_EAST_1;

            try (S3Client s3 = S3Client.builder()
                    .region(region)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build()) {
                // Multipart upload for large file
                uploadLargeFileToDeepGlacier(s3, s3BucketName, s3Path, dateArchiveFile);
                logger.info("Archive file {} - Uploaded COMPLETELY", dateArchiveFile.getName());

                if (!dateArchiveFile.delete()) {
                    logger.error("Archive file {} - COULD NOT DELETE ARCHIVE FILE", dateArchiveFile.getName());
                }
            }

        } catch (final IOException e) {
            logger.error("I/O exception happened", e);
            throw new RuntimeException("I/O exception happened", e);
        } finally {
            BM.stop();
        }
    }

    private void uploadLargeFileToDeepGlacier(final S3Client s3,
                                                     final String bucketName,
                                                     final String keyName,
                                                     final File file) throws IOException {
        long partSize = 5L * 1024L * 1024L; // Minimum part size is 5MB

        // Step 1: Initiate a multipart upload
        final CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .storageClass(StorageClass.DEEP_ARCHIVE)
                .build();
        final CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);
        final String uploadId = createResponse.uploadId();

        try {
            // Step 2: Upload parts
            final long fileLength = file.length();
            long position = 0;
            int partNumber = 1;
            final List<CompletedPart> completedParts = new ArrayList<>();

            while (position < fileLength) {
                final long bytesRemaining = fileLength - position;
                final long bytesToUpload = Math.min(partSize, bytesRemaining);

                final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build();

                try (final InputStream inputStream = new FileInputStream(file)) {
                    inputStream.skip(position);
                    final RequestBody requestBody = RequestBody.fromInputStream(inputStream, bytesToUpload);

                    final UploadPartResponse uploadPartResponse = s3.uploadPart(uploadPartRequest, requestBody);
                    completedParts.add(
                            CompletedPart.builder()
                                    .partNumber(partNumber)
                                    .eTag(uploadPartResponse.eTag())
                                    .build()
                    );
                }

                position += bytesToUpload;
                partNumber++;

                logger.info("Archive file {} - uploaded {}%", file, Math.round((position*100.0) / fileLength));
            }

            // Step 3: Complete the multipart upload
            final CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build();
            s3.completeMultipartUpload(completeRequest);
        } catch (final IOException e) {
            // Step 4: Abort the upload in case of failure
            final AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .build();
            s3.abortMultipartUpload(abortRequest);
            throw new IOException("Multipart upload failed: " + e.getMessage(), e);
        }
    }

    private static File[] listFolders(final File parent, final String pattern) {
        final File[] files = parent.listFiles(f -> f.isDirectory() && f.getName().matches(pattern));
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    private static File[] listFiles(final File parent) {
        final File[] files = parent.listFiles(File::isFile);
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }
}
