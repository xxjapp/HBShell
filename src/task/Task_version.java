package task;

import main.Version;
import common.utils.KDOs;

public class Task_version extends TaskBase {
    private static final int VERSION_MAJOR = 0;
    private static final int VERSION_MINOR = 2;

    @Override
    protected String description() {
        return "show version message";
    }

    @Override
    protected String usage() {
        return "version";
    }

    @Override
    public String example() {
        return "version";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0;
    }

    @Override
    public void execute() {
        printVersion();
    }

    private static void printVersion() {
        String lastChangedTime = getLastChangedTime();

        log.info("HBase Shell");
        log.info(" - Simple but powerful replacement for ./hbase shell");
        log.info(" - Designed especially for KeepData database");
        log.info(" - Enter 'help<RETURN>' for list of supported commands");
        log.info(" - Enter 'exit<RETURN>' to exit");
        log.info("");
        log.info(String.format(" Version           : %d.%d.%s", VERSION_MAJOR, VERSION_MINOR, Version.REVISION));
        log.info(String.format(" Built time        : %s", Version.BUILD_TIME));
        log.info(String.format(" Last changed time : %s", lastChangedTime));

        if (lastChangedTime.startsWith("2") && lastChangedTime.compareTo(Version.BUILD_TIME) > 0) {
            log.info(" * New version available * ");
        }
    }

    private static String getLastChangedTime() {
        String[] commandline = {
            "ruby",
            ".externalToolBuilders/last_changed_time.rb",
        };

        StringBuilder sbOutput = new StringBuilder();
        KDOs.runExternalExe(commandline, sbOutput);
        return sbOutput.toString().trim();
    }
}
