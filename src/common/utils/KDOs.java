package common.utils;

import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;

import common.Common;
import common.LogHelper;

public class KDOs {
    private static final Log log = LogHelper.getLog();

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

    public static int runExternalExe(List<String> commandline, StringBuilder sbOutput) {
        String[] array = commandline.toArray(new String[commandline.size()]);
        return runExternalExe(array, sbOutput);
    }

    public static int runExternalExe(String[] commandline, StringBuilder sbOutput) {
        log.info(Common.join(commandline, " "));

        String         exeName        = FilenameUtils.getName(commandline[0]);
        ProcessBuilder processBuilder = new ProcessBuilder(commandline);
        processBuilder.redirectErrorStream(true);

        String         line   = null;
        BufferedReader reader = null;

        try {
            Process process = processBuilder.start();
            setTimeout(process, PROCESS_TIMEOUT * 1000);

            InputStream stdout = process.getInputStream();
            reader = new BufferedReader(new InputStreamReader(stdout, CHARSET_NAME));

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
        } finally {
            closeQuietly(reader);
        }
    }

    private static void setTimeout(final Process process, long timeout) {
        final Timer timer     = new Timer();
        TimerTask   timerTask = new TimerTask() {
            @Override
            public void run() {
                timer.cancel();

                if (processIsAlive(process)) {
                    log.error("Kill process because of timeout!");
                    process.destroy();
                }
            }
        };

        timer.schedule(timerTask, timeout);
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
