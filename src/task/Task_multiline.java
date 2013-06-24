package task;

import java.io.IOException;

import main.HBShell;

public class Task_multiline extends TaskBase {
    @Override
    protected String description() {
        return "show current multiline status or set new multiline status temporarily\n" +
               "\n" +
               "** NOTE: for permanent change of multiline status, modify setting file\n" +
               "[conf/config.ini]";
    }

    @Override
    protected String usage() {
        return "multiline [0, false or 1, true]";
    }

    @Override
    public String example() {
        return "multiline false";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0 || argNumber == 1;
    }

    @Override
    protected void assignParam(String[] args)
    throws IOException {
        if (args.length == 1) {
            if (args[0].equals("0") || args[0].equals("false")) {
                levelParam.put(Level.OTHER, "false");
            } else if (args[0].equals("1") || args[0].equals("true")) {
                levelParam.put(Level.OTHER, "true");
            } else {
                throw new IOException("Invalid parameter value '" + args[0] + "'");
            }
        }
    }

    @Override
    public void execute()
    throws IOException {
        String status = (String) levelParam.get(Level.OTHER);

        if (status == null) {
            outputMultilineStatus();
        } else {
            setMultilineStatus(status);
        }
    }

    private void outputMultilineStatus()
    throws IOException {
        log.info("Current multiline status: " + HBShell.multiline);
    }

    private void setMultilineStatus(String status)
    throws IOException {
        HBShell.multiline = Boolean.valueOf(status);
        outputMultilineStatus();
    }
}

