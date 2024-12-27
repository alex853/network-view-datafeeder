package net.simforge.networkview.datafeeder.vatsim.json;

import com.google.gson.Gson;
import net.simforge.commons.io.IOHelper;
import net.simforge.commons.legacy.BM;
import net.simforge.commons.legacy.misc.Settings;
import net.simforge.commons.misc.Misc;
import net.simforge.commons.runtime.BaseTask;
import net.simforge.commons.runtime.RunningMarker;
import net.simforge.commons.runtime.ThreadMonitor;
import net.simforge.networkview.core.Network;
import net.simforge.networkview.core.report.ReportUtils;
import net.simforge.networkview.datafeeder.SettingNames;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Download extends BaseTask {
    private static final String ARG_PERIOD = "period";
    private static final String ARG_SINGLE = "single";
    private static final String ARG_STATUS_FILE_INTERVAL = "status-file-interval";

    private Network network;
    @SuppressWarnings("FieldCanBeLocal")
    private String networkStatusUrl;
    private List<String> sourceUrls;
    private String storageRoot = ReportJSONStorage.DEFAULT_STORAGE_ROOT;
    private ReportJSONStorage storage;
    private String lastReport;
    private int downloadPeriod = 110;
    private int statusFileInterval = 30;
    private boolean singleRun = false;
    private boolean endRequired = true;
    private String endSection = null;

    private Gson gson = new Gson();

    public Download(Properties properties) {
        super("Download-VATSIM-JSON");
        init(properties);
    }

    private void init(Properties properties) {
        network = Network.VATSIM;
        networkStatusUrl = "https://status.vatsim.net/status.json";
        endRequired = false;

        try {
            downloadPeriod = Integer.parseInt(properties.getProperty(ARG_PERIOD));
        } catch (Exception e) {
            //noop
        }
        if (downloadPeriod < 30) {
            downloadPeriod = 30;
        }

        storageRoot = Settings.get(SettingNames.storageRoot) != null ? Settings.get(SettingNames.storageRoot) : storageRoot;

        singleRun = Boolean.parseBoolean(properties.getProperty(ARG_SINGLE, Boolean.toString(singleRun)));

        try {
            statusFileInterval = Integer.parseInt(properties.getProperty(ARG_STATUS_FILE_INTERVAL));
        } catch (Exception e) {
            //noop
        }
    }

    @Override
    protected void startup() {
        super.startup();

        BM.setLoggingPeriod(TimeUnit.HOURS.toMillis(1));
//        BM.setLoggingPeriod(TimeUnit.MINUTES.toMillis(10));

        RunningMarker.lock(getTaskName());

        logger.info("Network        : " + network);
        logger.info("Storage root   : " + storageRoot);
        logger.info("Download period: " + downloadPeriod + " secs");
        logger.info("Single run     : " + singleRun);

        storage = ReportJSONStorage.getStorage(storageRoot, network);

        try {
            lastReport = storage.getLastReport();
        } catch (IOException e) {
            throw new RuntimeException("I/O error happened", e);
        }
        logger.info("LastReport     : " + lastReport);

        setBaseSleepTime(downloadPeriod * 1000L);
    }

    @Override
    protected void process() {
        BM.start("Download.process");
        try {
            loadStatusFile();
            downloadReport();
        } catch (IOException e) {
            logger.error("I/O error happened", e);
            throw new RuntimeException("I/O error happened", e);
        } finally {
            BM.stop();
        }
    }

    private void downloadReport() throws IOException {
        BM.start("Download.downloadReport");
        try {

            List<String> urls = new ArrayList<>();

            int iteration = 0;
            while (true) {
                ThreadMonitor.alive();

                iteration++;
                if (iteration > 10) {
                    logger.warn("All iterations done but actual data was not downloaded. Waiting for next download period");
                    break;
                }

                if (urls.isEmpty()) {
                    urls.addAll(sourceUrls);
                }

                int index = (int) (Math.random() * (double) urls.size());
                String url = urls.remove(index);

                String data;
                try {
                    logger.debug("Downloading from " + url + "...");
//                    data = IOHelper.download(url);
                    data = download(url);
                } catch (IOException e) {
                    logger.error("Can't download report", e);
                    Misc.sleepBM(500L);
                    continue;
                }
                logger.debug("Downloaded data size is " + data.length() + " bytes");

                ReportJSONFile reportFile;
                try {
                    reportFile = new ReportJSONFile(network, data);
                } catch (Exception e) {
                    logger.error("Error on parsing downloaded report, URL was " + url, e);

                    IOHelper.saveFile(new File("./Download-" + network + "-FAILED-REPORT-DATA.txt"), data);
                    logger.warn("FAILED-REPORT-DATA file has been saved");

                    Misc.sleepBM(500L);
                    continue;
                }

                String update = reportFile.getUpdate();
                logger.info(ReportUtils.log(update) + " - Downloaded");
                if (lastReport != null && !ReportUtils.isTimestampGreater(update, lastReport)) {
                    logger.warn(ReportUtils.log(update) + " - Downloaded data is not actual. Trying again...");
                    Misc.sleepBM(10000);
                    continue;
                }

                storage.saveReport(update, data);
                logger.debug(ReportUtils.log(update) + " - Downloaded data saved");
                lastReport = update;

                break;
            }

        } finally {
            BM.stop();
        }
    }

    private void loadStatusFile() throws IOException {
        BM.start("Download.loadStatusFile");
        try {

            File statusFile = new File(storage.getRoot(), "_status.json");
            String statusContent;


            boolean hasStatusFile = statusFile.exists();
            boolean statusFileExpired = hasStatusFile && statusFile.lastModified() + TimeUnit.DAYS.toMillis(statusFileInterval) < System.currentTimeMillis();
            boolean needToDownload = !hasStatusFile || statusFileExpired;


            if (needToDownload) {
                logger.info("Status file downloading...");
                while (true) {
                    try {
                        statusContent = download(networkStatusUrl);
                        break;
                    } catch (IOException e) {
                        logger.error("Can't download status file", e);
                        Misc.sleepBM(10000L);
                        logger.warn("Trying again to download status file...");
                    }
                }
                logger.info("Status file downloaded successfully");

                IOHelper.saveFile(statusFile, statusContent);
                logger.debug("Status file saved to disc");

                sourceUrls = getDataUrlList(statusContent);
            } else {
                if (sourceUrls == null) {
                    statusContent = IOHelper.loadFile(statusFile);
                    logger.info("Status file loaded from disk");

                    sourceUrls = getDataUrlList(statusContent);
                }
            }

        } finally {
            BM.stop();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes", "UnnecessaryLocalVariable"})
    private List<String> getDataUrlList(String statusContent) {
        Map<String, Object> statusMap = gson.fromJson(statusContent, Map.class);
        Map<String, Object> dataMap = (Map) statusMap.get("data");
        List<String> urls = (List<String>) dataMap.get("v3");
        return urls;
    }

    private String download(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection urlConnx = (HttpURLConnection) url.openConnection();
        urlConnx.setConnectTimeout(120000);
        urlConnx.setReadTimeout(120000);

        int responseCode = urlConnx.getResponseCode();
        boolean redirect = false;
        if (responseCode != HttpURLConnection.HTTP_OK) {
            if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP
                    || responseCode == HttpURLConnection.HTTP_MOVED_PERM
                    || responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
                redirect = true;
            }
        }

        if (redirect) {
            String newUrl = urlConnx.getHeaderField("Location");
            logger.warn("Redirected to URL : " + newUrl);
            urlConnx = (HttpURLConnection) new URL(newUrl).openConnection();
        }

        InputStream urlInputStream = urlConnx.getInputStream();
//        return readInputStreamWithTimeout(urlInputStream);
        return IOHelper.readInputStream(urlInputStream);
    }
}
