package task;

import java.io.IOException;

import main.HBShell;

public class TaskStatusBase extends TaskBase {
    private final String name;

    protected TaskStatusBase(String name) {
        this.name = name;
    }

    @Override
    protected String description() {
        return "show current " + name + " status or set new " + name + " status temporarily\n" +
               "\n" +
               "** NOTE: for permanent change of " + name + " status, modify setting file\n" +
               "[conf/config.ini]";
    }

    @Override
    protected String usage() {
        return name + " [0, false or 1, true]";
    }

    @Override
    public String example() {
        return name + " false";
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
            outputStatus();
        } else {
            setStatus(status);
        }
    }

    private void outputStatus()
    throws IOException {
        try {
            log.info("Current " + name + " status: " + HBShell.class.getField(name).get(null));
        } catch (Exception e) {
            log.error(name, e);
        }
    }

    protected void setStatus(String status)
    throws IOException {
        try {
            HBShell.class.getField(name).set(null, Boolean.valueOf(status));
        } catch (Exception e) {
            log.error(name, e);
        }

        outputStatus();
    }
}
