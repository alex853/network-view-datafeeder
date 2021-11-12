package net.simforge.networkview.datafeeder;

import net.simforge.networkview.core.report.persistence.ReportSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatafeederTasks {
    private static Logger logger = LoggerFactory.getLogger(DatafeederTasks.class.getName());

    private static ReportSessionManager reportSessionManager;

    public static class StartupAction implements Runnable {
        @Override
        public void run() {
            logger.info("creating session manager");
            reportSessionManager = new ReportSessionManager();
        }
    }

    public static class ShutdownAction implements Runnable {
        @Override
        public void run() {
            logger.info("killing session manager");
            ReportSessionManager _Report_sessionManager = reportSessionManager;
            reportSessionManager = null;
            _Report_sessionManager.dispose();
        }
    }

    public static ReportSessionManager getSessionManager() {
        return reportSessionManager;
    }
}
