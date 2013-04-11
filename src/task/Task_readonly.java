package task;

import java.io.IOException;

import main.HBShell;

public class Task_readonly extends TaskBase {
    @Override
    protected String description() {
        return "show current readonly status or set new readonly status temporarily\n" +
               "\n" +
               "** NOTE: for permanent change of readonly status, modify setting file\n" +
               "[conf/config.ini]";
    }

    @Override
    protected String usage() {
        return "readonly [0, false or 1, true]";
    }

    @Override
    public String example() {
        return "readonly false";
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
            outputReadonlyStatus();
        } else {
            setReadonlyStatus(status);
        }
    }

    private void outputReadonlyStatus()
    throws IOException {
        log.info("Current readonly status: " + HBShell.readonly);
    }

    private void setReadonlyStatus(String status)
    throws IOException {
        HBShell.readonly = Boolean.valueOf(status);
        outputReadonlyStatus();
    }
}
