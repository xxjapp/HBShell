package task;

import java.io.File;
import java.io.IOException;
import java.util.List;

import main.HBShell;

import org.apache.commons.io.FileUtils;

import common.Common;

import utils.MyStringTokenizer;

public class Task_run extends TaskBase {
    private String cmdFilePath = null;

    @Override
    protected String description() {
        return "run external hbshell command file";
    }

    @Override
    protected String usage() {
        return "run file_path";
    }

    @Override
    public String example() {
        return "run ./commands.hbc";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 1;
    }

    @Override
    protected void assignParam(String[] args) {
        cmdFilePath = args[0];
    }

    @Override
    public void execute()
    throws IOException {
        File         file      = new File(cmdFilePath);
        List<String> lines     = FileUtils.readLines(file, "UTF-8");
        long         lineIndex = 0;
        long         cmdIndex  = 0;

        for (String line : lines) {
            lineIndex++;

            if (lineIndex == 1) {
                line = Common.removeUtf8Bom(line);
            }

            line = line.trim();

            if (line.length() == 0 || line.startsWith("#")) {
                continue;
            }

            String[] cmdArgs = getCmdArgs(line);

            if (cmdArgs.length == 0) {
                continue;
            }

            HBShell.resetAllCount();

            log.info(String.format("#%d command - %s", ++cmdIndex, line));

            try {
                HBShell.doTask(cmdArgs);
            } catch (Exception e) {         // all exceptions
                log.error(null, e);
            }

            log.info("");
        }
    }

    private static String[] getCmdArgs(String line) {
        return MyStringTokenizer.getTokens(line);
    }
}
