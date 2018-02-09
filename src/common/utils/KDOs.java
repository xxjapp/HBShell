package common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;

import utils.RootLog;

import common.Common;

public class KDOs {
    private static final Log log = RootLog.getLog();

    private static final long PROCESS_TIMEOUT = 10 * 60;

    private static Boolean isUnixLike = null;

    public static boolean isUnixLike() {
        if (isUnixLike == null) {
            isUnixLike = !isWindows();
        }

        return isUnixLike;
    }

    public static boolean isWindows() {
        String os = System.getProperty("os.name");
        return os != null &&os.startsWith("Windows");
    }

    //
    // run external executables
    //

    private static final String CHARSET_NAME = KDOs.isUnixLike() ? "UTF-8" : "Shift_JIS";

    public static int runExternalExe(String[] commandline, StringBuilder sbOutput) {
        log.info(Common.join(commandline, " "));

        String         line           = null;
        String         exeName        = FilenameUtils.getName(commandline[0]);
        ProcessBuilder processBuilder = new ProcessBuilder(commandline);
        Process        process        = null;

        processBuilder.redirectErrorStream(true);

        try {
            process = processBuilder.start();
        } catch (IOException e) {
            log.error(null, e);
            return -1;
        }

        try (InputStream stdout = process.getInputStream(); BufferedReader reader = new BufferedReader(new InputStreamReader(stdout, CHARSET_NAME))) {
            setTimeout(process, PROCESS_TIMEOUT * 1000);

            log.info("-------- " + exeName + " log start --------");

            while ((line = reader.readLine()) != null) {
                log.info(line);

                if (sbOutput != null) {
                    sbOutput.append(line).append("\n");
                }
            }

            log.info("-------- " + exeName + " log end ----------");

            return process.waitFor();
        } catch (Exception e) {
            log.error(null, e);
            return -1;
        }
    }

    private static void setTimeout(final Process process, final long timeout) {
        final long  start = System.currentTimeMillis();
        final Timer timer = new Timer();

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                boolean processIsAlive = processIsAlive(process);

                if (!processIsAlive) {
                    timer.cancel();
                } else {
                    long now = System.currentTimeMillis();

                    if (now - start > timeout) {
                        log.error("Kill process because of timeout!");
                        process.destroy();

                        timer.cancel();
                    }
                }
            }
        };

        timer.schedule(timerTask, 0, 1000);        // check every second
    }

    private static boolean processIsAlive(Process process) {
        try {
            process.exitValue();
            return false;
        } catch (IllegalThreadStateException e) {
            return true;
        }
    }
}
