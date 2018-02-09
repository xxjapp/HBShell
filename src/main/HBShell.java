package main;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;

import jline.ConsoleReader;

import org.apache.commons.io.FileUtils;

import sun.misc.Signal;
import sun.misc.SignalHandler;
import task.Task;
import task.TaskBase;
import task.TaskBase.TaskType;
import task.Task_history;
import utils.MyConsoleReader;
import utils.MyStringTokenizer;
import utils.PropertiesHelper;
import utils.ResultLog;
import utils.RootLog;
import utils.Utils;

import common.Common;

public class HBShell {
    private static final ResultLog log = ResultLog.getLog();

    public static final String HISTORY_FILE = Utils.makePath(RootLog.logDir, "history.txt");

    private static final String CONFIRM_YES    = "yes";
    private static final String ENCODING       = "UTF-8";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private enum SessionMode {
        single,
        multi,
    }

    private static final String MAIN_CONF_FILE = "./conf/config.ini";

    private static final File historyFile = new File(HISTORY_FILE);

    // config default values
    public static Long    maxPrintableDetectCnt   = 1000L;
    public static Long    maxHexStringLength      = 8L;
    public static Boolean travelRowFBlockFamilies = true;
    public static Boolean readonly                = true;
    public static Boolean multiline               = false;
    public static Boolean showtimestamp           = true;
    public static Boolean showvaluelength         = false;
    public static Boolean usefamilycache          = true;
    public static Long    maxResultLogFileCount   = 10L;
    public static Long    defaultHistoryCount     = 30L;
    public static String  binaryDataDir           = "./bin_data";

    public static List<String> alias_clear           = Arrays.asList("cle", "clr");
    public static List<String> alias_connect         = Arrays.asList("con");
    public static List<String> alias_count           = Arrays.asList("cnt");
    public static List<String> alias_create          = Arrays.asList("c", "cre");
    public static List<String> alias_delete          = Arrays.asList("d", "del");
    public static List<String> alias_describe        = Arrays.asList("des");
    public static List<String> alias_export          = Arrays.asList("ex");
    public static List<String> alias_filter          = Arrays.asList("f");
    public static List<String> alias_get             = Arrays.asList("g");
    public static List<String> alias_help            = Arrays.asList("h");
    public static List<String> alias_history         = Arrays.asList("his");
    public static List<String> alias_import          = Arrays.asList("im");
    public static List<String> alias_list            = Arrays.asList("l", "ls");
    public static List<String> alias_multiline       = Arrays.asList("m");
    public static List<String> alias_put             = Arrays.asList("p");
    public static List<String> alias_quit            = Arrays.asList("e", "q", "exit");
    public static List<String> alias_readonly        = Arrays.asList("ro");
    public static List<String> alias_reg_delete      = Arrays.asList("rd");
    public static List<String> alias_rename          = Arrays.asList("rn", "ren");
    public static List<String> alias_run             = Arrays.asList("r");
    public static List<String> alias_scan            = Arrays.asList("s");
    public static List<String> alias_showtimestamp   = Arrays.asList("st");
    public static List<String> alias_showvaluelength = Arrays.asList("sl");
    public static List<String> alias_usefamilycache  = Arrays.asList("fc");
    public static List<String> alias_version         = Arrays.asList("v", "ver");

    public static String format_table         = "T: %s";
    public static String format_row           = " R: %s";
    public static String format_family        = "  F: %s";
    public static String format_qualifier     = "   Q: %-20s";
    public static String format_qualifierOmit = "   %s";
    public static String format_timestamp     = "[yyyy-MM-dd HH:mm:ss]";
    public static String format_valuelength   = "[%08d]";
    public static String format_value         = "%s";

    private static SessionMode   sessionMode   = null;
    private static Task          currentTask   = null;
    private static Scanner       inputScanner  = null; // for windows
    private static ConsoleReader consoleReader = null; // for linux
    private static String        lastCmd       = Task_history.getLastCmd();

    private static final Map<String, Long> countMap = new HashMap<>();

    public static final String TABLE     = "table";
    public static final String ROW       = "row";
    public static final String FAMILY    = "family";
    public static final String QUALIFIER = "qualifier";
    public static final String VALUE     = "value";

    private static void init() {
        // read main configure file
        Properties properties = PropertiesHelper.getProperties(MAIN_CONF_FILE);

        maxPrintableDetectCnt   = PropertiesHelper.getProperty(properties, "maxPrintableDetectCnt",     maxPrintableDetectCnt);
        maxHexStringLength      = PropertiesHelper.getProperty(properties, "maxHexStringLength",        maxHexStringLength);
        travelRowFBlockFamilies = PropertiesHelper.getProperty(properties, "travelRowFBlockFamilies",   travelRowFBlockFamilies);
        readonly                = PropertiesHelper.getProperty(properties, "readonly",                  readonly);
        multiline               = PropertiesHelper.getProperty(properties, "multiline",                 multiline);
        showtimestamp           = PropertiesHelper.getProperty(properties, "showtimestamp",             showtimestamp);
        showvaluelength         = PropertiesHelper.getProperty(properties, "showvaluelength",           showvaluelength);
        usefamilycache          = PropertiesHelper.getProperty(properties, "usefamilycache",            usefamilycache);
        maxResultLogFileCount   = PropertiesHelper.getProperty(properties, "maxResultLogFileCount",     maxResultLogFileCount);
        defaultHistoryCount     = PropertiesHelper.getProperty(properties, "defaultHistoryCount",       defaultHistoryCount);
        binaryDataDir           = PropertiesHelper.getProperty(properties, "binaryDataDir",             binaryDataDir);

        alias_clear           = PropertiesHelper.getProperty(properties, "alias_clear",           alias_clear);
        alias_connect         = PropertiesHelper.getProperty(properties, "alias_connect",         alias_connect);
        alias_count           = PropertiesHelper.getProperty(properties, "alias_count",           alias_count);
        alias_create          = PropertiesHelper.getProperty(properties, "alias_create",          alias_create);
        alias_delete          = PropertiesHelper.getProperty(properties, "alias_delete",          alias_delete);
        alias_describe        = PropertiesHelper.getProperty(properties, "alias_describe",        alias_describe);
        alias_export          = PropertiesHelper.getProperty(properties, "alias_export",          alias_export);
        alias_filter          = PropertiesHelper.getProperty(properties, "alias_filter",          alias_filter);
        alias_get             = PropertiesHelper.getProperty(properties, "alias_get",             alias_get);
        alias_help            = PropertiesHelper.getProperty(properties, "alias_help",            alias_help);
        alias_history         = PropertiesHelper.getProperty(properties, "alias_history",         alias_history);
        alias_import          = PropertiesHelper.getProperty(properties, "alias_import",          alias_import);
        alias_list            = PropertiesHelper.getProperty(properties, "alias_list",            alias_list);
        alias_multiline       = PropertiesHelper.getProperty(properties, "alias_multiline",       alias_multiline);
        alias_put             = PropertiesHelper.getProperty(properties, "alias_put",             alias_put);
        alias_quit            = PropertiesHelper.getProperty(properties, "alias_quit",            alias_quit);
        alias_readonly        = PropertiesHelper.getProperty(properties, "alias_readonly",        alias_readonly);
        alias_reg_delete      = PropertiesHelper.getProperty(properties, "alias_reg_delete",      alias_reg_delete);
        alias_rename          = PropertiesHelper.getProperty(properties, "alias_rename",          alias_rename);
        alias_run             = PropertiesHelper.getProperty(properties, "alias_run",             alias_run);
        alias_scan            = PropertiesHelper.getProperty(properties, "alias_scan",            alias_scan);
        alias_showtimestamp   = PropertiesHelper.getProperty(properties, "alias_showtimestamp",   alias_showtimestamp);
        alias_showvaluelength = PropertiesHelper.getProperty(properties, "alias_showvaluelength", alias_showvaluelength);
        alias_usefamilycache  = PropertiesHelper.getProperty(properties, "alias_usefamilycache",  alias_usefamilycache);
        alias_version         = PropertiesHelper.getProperty(properties, "alias_version",         alias_version);

        format_table         = removeQuotes(PropertiesHelper.getProperty(properties, "format_table",         format_table));
        format_row           = removeQuotes(PropertiesHelper.getProperty(properties, "format_row",           format_row));
        format_family        = removeQuotes(PropertiesHelper.getProperty(properties, "format_family",        format_family));
        format_qualifier     = removeQuotes(PropertiesHelper.getProperty(properties, "format_qualifier",     format_qualifier));
        format_qualifierOmit = removeQuotes(PropertiesHelper.getProperty(properties, "format_qualifierOmit", format_qualifierOmit));
        format_timestamp     = removeQuotes(PropertiesHelper.getProperty(properties, "format_timestamp",     format_timestamp));
        format_valuelength   = removeQuotes(PropertiesHelper.getProperty(properties, "format_valuelength",   format_valuelength));
        format_value         = removeQuotes(PropertiesHelper.getProperty(properties, "format_value",         format_value));

        // add signal handler
        // NOTE: No replacement found at present for these SUN private APIs
        Signal.handle(new Signal("INT"), new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                if (currentTask != null) {
                    currentTask.cancel();
                }
            }
        });
    }

    private static String removeQuotes(String string) {
        return string.substring(1, string.length() - 1);
    }

    private static void exit() {
        closeInputScanner();
    }

    public static void doTask(String[] cmdArgs)
    throws IOException {
        TaskType taskType = TaskBase.getTaskType(cmdArgs[0]);

        String[] args = new String[cmdArgs.length - 1];
        System.arraycopy(cmdArgs, 1, args, 0, cmdArgs.length - 1);        // remove first arg(task type)

        currentTask = TaskBase.getTask(taskType);
        currentTask.doTask(args);
        currentTask = null;
    }

    private static void printHelp() {
        log.info("Usage: ");
        log.info("1) start shell of HBShell");
        log.info("#  ruby run.rb");
        log.info("");
        log.info("2) execute a single HBShell command");
        log.info("#  ruby run.rb -c \"{command}\"");
        log.info("");
        log.info("3) print version information");
        log.info("#  ruby run.rb -v");
        log.info("");
        log.info("4) print this message");
        log.info("#  ruby run.rb -h");
        log.info("");
    }

    public static void main(String[] args)
    throws IOException {
        init();

        String commandlineCommand = null;

        if (args.length == 0) {
            sessionMode = SessionMode.multi;
        } else if (args.length == 2 && args[0].equals("-c")) {
            sessionMode        = SessionMode.single;
            commandlineCommand = args[1];
        } else if (args.length == 1 && args[0].equals("-v")) {
            sessionMode        = SessionMode.single;
            commandlineCommand = "version";
        } else {
            printHelp();
            return;
        }

        do {
            String[] cmdArgs = null;

            try {
                cmdArgs = getCmdArgs(commandlineCommand);
            } catch (Exception e) {         // all exceptions
                log.error(null, e);
                continue;
            }

            if (sessionMode == SessionMode.multi && cmdArgs != null &&cmdArgs.length == 0) {
                continue;
            }

            Date start = new Date();

            resetAllCount();

            try {
                doTask(cmdArgs);
            } catch (Exception e) {         // all exceptions
                log.error(null, e);
                continue;
            }

            Date stop = new Date();

            double timeUsed = (stop.getTime() - start.getTime()) / 1000.0;

            log.info("---------------------------------------");

            if (countMap.get(TABLE) != null) {
                log.info("CNT_TABLE     ：" + countMap.get(TABLE));
            }

            if (countMap.get(ROW) != null) {
                log.info("CNT_ROW       ：" + countMap.get(ROW));
            }

            if (countMap.get(FAMILY) != null) {
                log.info("CNT_FAMILY    ：" + countMap.get(FAMILY));
            }

            if (countMap.get(QUALIFIER) != null) {
                log.info("CNT_QUALIFIER ：" + countMap.get(QUALIFIER));
            }

            if (countMap.get(VALUE) != null) {
                log.info("CNT_VALUE     ：" + countMap.get(VALUE));
            }

            log.stopLogToFile();
            log.info("");
            log.info("時間          ：" + timeUsed + " [sec]");
            log.info("");

            historyAdd(cmdArgs);
        } while (sessionMode == SessionMode.multi);

        exit();
    }

    private static void historyAdd(String[] cmdArgs)
    throws IOException {
        String cmd = Common.join(cmdArgs, " ");

        if (!cmd.equals(lastCmd)) {
            lastCmd = cmd;
            FileUtils.writeStringToFile(historyFile, cmd + LINE_SEPARATOR, ENCODING, true);
        }
    }

    public static boolean confirmFor(String message)
    throws IOException {
        String userInput = getUserInput(message + " \t" + CONFIRM_YES + "/[no] : ");
        return userInput.equals(CONFIRM_YES);
    }

    public static void resetAllCount() {
        countMap.clear();
    }

    public static void increaseCount(String key) {
        increaseCount(key, 1);
    }

    public static void increaseCount(String key, long count) {
        Long oldCount = countMap.get(key);

        if (oldCount == null) {
            oldCount = 0L;
        }

        countMap.put(key, oldCount + count);
    }

    public static Long getCount(String key) {
        Long count = countMap.get(key);

        if (count == null) {
            return 0L;
        }

        return count;
    }

    private static String[] getCmdArgs(String commandlineCommand)
    throws IOException {
        if (sessionMode == SessionMode.multi) {
            return MyStringTokenizer.getTokens(getUserInput("> "));
        }

        if (sessionMode == SessionMode.single) {
            return commandlineCommand.split(" ");
        }

        return null;
    }

    private static String getUserInput(String prompt)
    throws IOException {
        String line = null;

        if (!Utils.isWindows()) {
            // No exception can be caught, so nothing changes when user press ^C in linux
            line = getConsoleReader().readLine(prompt);
        } else {
            System.out.print(prompt);

            try {
                line = getInputScanner().nextLine();
            } catch (NoSuchElementException e) {
                // user may press ^C (first input)
                // the following lines may not be run
                forceDiscardInputScanner();
                System.out.println();
                return "";
            }
        }

        return line;
    }

    private static Scanner getInputScanner() {
        if (inputScanner != null) {
            return inputScanner;
        }

        inputScanner = new Scanner(System.in);
        return inputScanner;
    }

    private static void forceDiscardInputScanner() {
        inputScanner = null;
    }

    private static void closeInputScanner() {
        if (inputScanner != null) {
            inputScanner.close();
            inputScanner = null;
        }
    }

    private static ConsoleReader getConsoleReader()
    throws IOException {
        if (consoleReader != null) {
            return consoleReader;
        }

        consoleReader = new MyConsoleReader();
        consoleReader.setBellEnabled(false);    // bell does not work correctly, so disable it

        return consoleReader;
    }
}
