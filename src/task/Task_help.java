package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Task_help extends TaskBase {
    @Override
    protected String description() {
        return "show help message";
    }

    @Override
    protected String usage() {
        return "help [topic1 [topic2 ...]]";
    }

    @Override
    public String example() {
        return "help filter get";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 0;
    }

    @Override
    protected void assignParam(String[] args) {
        List<String> topics = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            topics.add(args[i]);
        }

        levelParam.put(Level.OTHER, topics);
    }

    @Override
    public void execute()
    throws IOException {
        List< ? > topics = (List< ? >)levelParam.get(Level.OTHER);

        if (topics.isEmpty()) {
            printAllHelp();
        } else {
            printHelpOn(topics);
        }
    }

    private void printAllHelp() {
        for (TaskType taskType : TaskType.values()) {
            printHelpOn(taskType);

            if (isCancelled()) {
                return;
            }
        }

        printSpecialNote();
    }

    private static void printSpecialNote() {
        log.info("## NOTE 1) - Keyboard in linux");
        log.info("");
        log.info("     - all control keys are not usable before jline added");
        log.info("     - thanks to jline, arrow left/right/up/down are usable, but");
        log.info("     - backspace and delete are switched (resolved by MyUnixTerminal)");
        log.info("     - home/end, page up/down are not usable (resolved by MyConsoleReader partially)");
        log.info("      - the following text in pasting text will act as control keys");
        log.info("       - '1~' -> home, go to begin of line");
        log.info("       - '2~' -> insert, do nothing");
        log.info("       - '4~' -> end, go to end of line");
        log.info("       - '5~' -> page up, move to first history entry");
        log.info("       - '6~' -> page down, move to last history entry");
        log.info("");
        log.info("## NOTE 2) - Command modifier/row limit");
        log.info("");
        log.info("     - all commands can be added with a row limit number to only operate on first found rows");
        log.info("     - e.g. scan10 table . family qualifier value");
        log.info("");
        log.info("## NOTE 3) - Command modifier/all-handle mode");
        log.info("");
        log.info("     - all commands can be added with a '*' mark to execute without omitting series of qualifiers, thus 'f0, ..., f3' => 'f0, f1, f2, f3'");
        log.info("     - some commands always use this mode, eg. 'export'");
        log.info("     - e.g. get* table key");
        log.info("");
        log.info("## NOTE 4) - Command modifier/quiet mode");
        log.info("");
        log.info("     - all commands can be added with a '-' mark to execute without logging to console and normal log file");
        log.info("     - e.g. scan- table");
        log.info("");
        log.info("## NOTE 5) - Command modifier/no result file mode");
        log.info("");
        log.info("     - all commands can be added with a '^' mark to execute without logging result file");
        log.info("     - e.g. scan^ table");
        log.info("");
        log.info("## NOTE 6) - Command modifier/force to execute");
        log.info("");
        log.info("     - all commands can be added with a '!' mark to execute without confirmation");
        log.info("     - e.g. delete! table key family qualifier");
        log.info("");
        log.info("## NOTE 7) - Command modifier/increase put");
        log.info("");
        log.info("     - put commands can be added with a '+' mark to put a numerical value added to the old numerical value");
        log.info("     - e.g. put+ table key family qualifier 22");
        log.info("");
        log.info("## NOTE 8) - Command modifier/append put");
        log.info("");
        log.info("     - put commands can be added with a '~' mark to append value to the old value");
        log.info("     - e.g. put~ table key family qualifier value");
        log.info("");
    }

    private static void printHelpOn(List< ? > topics)
    throws IOException {
        for (Object topic : topics) {
            TaskType taskType = getTaskType(topic.toString());
            printHelpOn(taskType);
        }
    }

    private static void printHelpOn(TaskType taskType) {
        Task task = TaskBase.getTask(taskType);
        task.printHelp();
    }
}
