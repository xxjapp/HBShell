package task;

import java.util.List;

import main.Version;
import utils.Utils;

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
        log.info("HBShell");
        log.info(" - Simple but powerful replacement for ./hbase shell");
        log.info(" - Designed especially for KeepData database");
        log.info(" - Enter 'help<RETURN>' for list of supported commands");
        log.info(" - Enter 'exit<RETURN>' to exit");
        log.info("");
        log.info(String.format(" Version           : %d.%d.%s", VERSION_MAJOR, VERSION_MINOR, Version.REVISION));
        log.info(String.format(" Commit time       : %s", Version.COMMIT_TIME));

        if (Long.valueOf(Version.REVISION) < getNewestRevision()) {
            log.info(" * New version available * ");
        }
    }

    private static long getNewestRevision() {
        String[] commandline = {
            "curl",
            "--insecure",
            "https://raw.githubusercontent.com/xxjapp/HBShell/master/src/main/Version.java",
        };

        StringBuilder sbOutput = new StringBuilder();
        KDOs.runExternalExe(commandline, sbOutput);

        String       output = sbOutput.toString();
        List<String> groups = Utils.match(output, "REVISION *= *\"(\\d+)\";");

        try {
            return Long.valueOf(groups.get(1));
        } catch (IndexOutOfBoundsException e) {
            return 0;
        }
    }
}
