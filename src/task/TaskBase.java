package task;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.HBShell;

import org.apache.hadoop.hbase.client.HTableInterface;

import tnode.TNodeBase;
import tnode.TNodeDatabase;
import tnode.TNodeRow;
import tnode.TNodeTable;
import utils.ResultLog;
import utils.Utils;
import exception.HBSException;
import exception.HBSExceptionRowLimitReached;

public abstract class TaskBase implements Task {
    protected static final ResultLog log = ResultLog.getLog();

    public enum TaskType {
        CLEAR,
        CONNECT,
        COUNT,
        CREATE,
        DELETE,
        DESCRIBE,
        EXPORT,
        FILTER,
        GET,
        HELP,
        HISTORY,
        IMPORT,
        LIST,
        MULTILINE,
        PUT,
        QUIT,
        READONLY,
        REG_DELETE,
        RENAME,
        RUN,
        SCAN,
        SHOWTIMESTAMP,
        SHOWVALUELENGTH,
        USEFAMILYCACHE,
        VERSION,
    }

    public enum Level {
        TABLE,
        ROW,
        FAMILY,
        QUALIFIER,
        VALUE,
        OTHER,
    }

    private static final String CLASS_NAME_PREFIX = "task.Task_";

    public Map<Level, Object> levelParam = new HashMap<>();
    public Level              level      = null;

    private boolean notifyEnabled = false;
    private boolean needConfirm   = false;
    private boolean cancelled     = false;

    private static Map<String, TaskType> aliasMap = null;

    // command modifiers
    private static boolean forced       = false;
    private static boolean quiet        = false;
    private static boolean noResultFile = false;
    private static boolean handleAll    = false;
    private static boolean increasePut  = false;
    private static boolean appendPut    = false;
    private static long    rowLimit     = Long.MAX_VALUE;

    private TaskType taskType = null;

    @Override
    public final void printHelp() {
        log.info("### " + getTaskType() + " - " + description());
        log.info("");
        log.info("      usage   : " + usage());
        log.info("      example : " + example());
        log.info("      alias   : " + alias());
        log.info("");
    }

    protected abstract String description();
    protected abstract String usage();

    @Override
    public List< ? > alias() {
        String aliasName = "alias_" + getTaskName();

        try {
            Field field = HBShell.class.getField(aliasName);
            return (List< ? >)field.get(null);
        } catch (Exception e) {     // all exceptions
            log.warn(null, e);
        }

        return null;
    }

    @Override
    public final void doTask(String[] args)
    throws IOException {
        changeLogOnStart();

        if (HBShell.readonly && !isReadOnly()) {
            throw new IOException("Non-readonly tasks not allowed in readonly mode\nEnter 'help readonly<RETURN>' for more information");
        }

        parseArgs(args);

        resetAllCount();

        log.setQuiet(quiet);
        log.setNoResultFile(noResultFile);

        // output
        outputParam();

        if (!doConfirm()) {
            return;
        }

        try {
            execute();
        } catch (HBSExceptionRowLimitReached e) {
            // OK
        } catch (HBSException e) {
            log.error(null, e);
        } finally {
            log.setQuiet(false);
        }
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean outpuBinary() {
        return false;
    }

    @Override
    public boolean isHandleAll() {
        return handleAll;
    }

    @Override
    public boolean isIncreasePut() {
        return increasePut;
    }

    @Override
    public boolean isAppendPut() {
        return appendPut;
    }

    @Override
    public long getRowLimit() {
        return rowLimit;
    }

    private boolean doConfirm()
    throws IOException {
        if (forced || !needConfirm) {
            return true;
        }

        boolean notifyEnabled = this.notifyEnabled;
        this.notifyEnabled = false;

        try {
            confirm();
        } catch (HBSExceptionRowLimitReached e) {
            // OK
        } catch (HBSException e) {
            log.error(null, e);
        }

        this.notifyEnabled = notifyEnabled;

        System.out.print("******************************\n");
        boolean confirmed = HBShell.confirmFor("Sure to " + getTaskType() + "?");
        System.out.print("******************************\n");

        return confirmed;
    }

    @Override
    public void resetAllCount() {
        HBShell.resetAllCount();
    }

    @Override
    public void changeLogOnStart() {
        log.startNew();
    }

    protected final void parseArgs(String[] args)
    throws IOException {
        if (!checkArgNumber(args.length)) {
            throw new IOException("Invalid argument number '" + args.length + "'");
        }

        // levelParam
        assignParam(args);

        // level
        this.level = getLevel();

        // needConfirm
        this.needConfirm = needConfirm();

        // notifyEnabled
        this.notifyEnabled = notifyEnabled();
    }

    protected abstract boolean checkArgNumber(int argNumber);

    /**
     * @throws IOException
     */
    protected void assignParam(String[] args)
    throws IOException {
        try {
            levelParam.put(Level.TABLE,     Pattern.compile(args[0]));
            levelParam.put(Level.ROW,       Pattern.compile(args[1]));
            levelParam.put(Level.FAMILY,    Pattern.compile(args[2]));
            levelParam.put(Level.QUALIFIER, Pattern.compile(args[3]));
            levelParam.put(Level.VALUE,     Pattern.compile(args[4]));
        } catch (ArrayIndexOutOfBoundsException e) {
            // OK
        }
    }

    @Override
    public Level getLevel() {
        return null;
    }

    @Override
    public boolean needConfirm() {
        return false;
    }

    @Override
    public boolean notifyEnabled() {
        return false;
    }

    @Override
    public boolean isToOutput() {
        return true;
    }

    private void outputParam() {
        log.info("taskType        : " + getTaskType());

        if (level != null) {
            log.info("level           : " + level);
        }

        if (levelParam.get(Level.TABLE) != null) {
            log.info("param-Table     : " + levelParam.get(Level.TABLE));
        }

        if (levelParam.get(Level.ROW) != null) {
            log.info("param-RowKey    : " + levelParam.get(Level.ROW));
        }

        if (levelParam.get(Level.FAMILY) != null) {
            log.info("param-Family    : " + levelParam.get(Level.FAMILY));
        }

        if (levelParam.get(Level.QUALIFIER) != null) {
            log.info("param-Qualifier : " + levelParam.get(Level.QUALIFIER));
        }

        if (levelParam.get(Level.VALUE) != null) {
            log.info("param-Value     : " + levelParam.get(Level.VALUE));
        }

        if (levelParam.get(Level.OTHER) != null) {
            log.info("param-Other     : " + levelParam.get(Level.OTHER));
        }

        log.info("---------------------------------------");
    }

    public void confirm()
    throws IOException, HBSException {
        execute();
    }

    public void execute()
    throws IOException, HBSException {
        new TNodeDatabase(this, isToOutput()).handle();
    }

    //
    // utils
    //

    private static String getTaskClassName(TaskType taskType) {
        return CLASS_NAME_PREFIX + taskType.toString().toLowerCase();
    }

    private TaskType getTaskType() {
        if (taskType != null) {
            return taskType;
        }

        this.taskType = getAliasMap().get(getTaskName());
        return taskType;
    }

    private String getTaskName() {
        String className = getClass().getName();
        return className.substring(CLASS_NAME_PREFIX.length());
    }

    public static final TaskType getTaskType(String string)
    throws IOException {
        String   command  = parseCommand(string);
        TaskType taskType = getAliasMap().get(command);

        if (taskType == null) {
            throw new IOException("Undefined command '" + command.toUpperCase() + "'");
        }

        return taskType;
    }

    private static void initCommandModifiers() {
        forced       = false;
        quiet        = false;
        noResultFile = false;
        handleAll    = false;
        increasePut  = false;
        appendPut    = false;
        rowLimit     = Long.MAX_VALUE;
    }

    private static String parseCommand(String string) {
        initCommandModifiers();

        while (true) {
            String string0 = string;

            // check if forced
            if (string.endsWith("!")) {
                forced = true;
                string = string.substring(0, string.length() - 1);
            }

            // check if quiet
            if (string.endsWith("-")) {
                quiet  = true;
                string = string.substring(0, string.length() - 1);
            }

            // check if do not record to result file
            if (string.endsWith("^")) {
                noResultFile = true;
                string       = string.substring(0, string.length() - 1);
            }

            // check if handle all
            if (string.endsWith("*")) {
                handleAll = true;
                string    = string.substring(0, string.length() - 1);
            }

            // check if this is an increase put
            if (string.endsWith("+")) {
                increasePut = true;
                string      = string.substring(0, string.length() - 1);
            }

            // check if this is an append put
            if (string.endsWith("~")) {
                appendPut = true;
                string    = string.substring(0, string.length() - 1);
            }

            // get row limit parameter
            List<String> groups = Utils.match(string, "(\\d+)$");

            if (groups.size() == 2) {
                String g1 = groups.get(1);
                rowLimit = Long.valueOf(g1);
                string   = string.substring(0, string.length() - g1.length());
            }

            if (string == string0) {
                return string;
            }
        }
    }

    public static final Task getTask(TaskType taskType) {
        try {
            String     taskClassName = getTaskClassName(taskType);
            Class< ? > clazz         = Class.forName(taskClassName);

            Class< ? > []    parameterTypes = new Class[] {};
            Constructor< ? > constructor    = clazz.getConstructor(parameterTypes);

            return (Task) constructor.newInstance(new Object[] {});
        } catch (Exception e) {         // all exceptions
            // a lot of exceptions, but there should be no errors if all taskType implemented
            log.error(null, e);
            return null;
        }
    }

    public static Map<String, TaskType> getAliasMap() {
        if (aliasMap != null) {
            return aliasMap;
        }

        aliasMap = new HashMap<>();

        for (TaskType taskType : TaskType.values()) {
            Task      task    = TaskBase.getTask(taskType);
            List< ? > aliases = task.alias();

            for (Object alias : aliases) {
                aliasMap.put((String) alias, taskType);
            }

            aliasMap.put(taskType.toString().toLowerCase(), taskType);
        }

        return aliasMap;
    }

    //
    // notify
    //

    @SuppressWarnings("resource")
    public void notifyFound(TNodeBase node)
    throws IOException, HBSException {
        if (!notifyEnabled) {
            return;
        }

        HTableInterface table = null;

        switch (node.level) {
        case TABLE:
            table = ((TNodeTable)node).getTable();

            try {
                foundTable(table);
            } finally {
                ((TNodeTable)node).closeTable(table);
            }

            break;

        case ROW:
            table = ((TNodeRow)node).table;
            foundRow(table, node.name);
            break;

        case FAMILY:
            table = ((TNodeRow)node.parent).table;
            foundFamily(table, node.parent.name, node.name);
            break;

        case QUALIFIER:
            table = ((TNodeRow)node.parent.parent).table;
            foundQualifier(table, node.parent.parent.name, node.parent.name, node.name);
            break;

        case VALUE:
            table = ((TNodeRow)node.parent.parent.parent).table;
            foundValue(table, node.parent.parent.parent.name, node.parent.parent.name, node.parent.name, node.name);
            break;

        default:
            break;
        }
    }

    /**
     * @param table
     * @throws IOException
     * @throws HBSException
     */
    protected void foundTable(HTableInterface table)
    throws IOException, HBSException {
        // Do nothing
    }

    /**
     * @param table
     * @param row
     * @throws IOException
     */
    protected void foundRow(HTableInterface table, String row)
    throws IOException {
        // Do nothing
    }

    /**
     * @param table
     * @param row
     * @param family
     * @throws IOException
     */
    protected void foundFamily(HTableInterface table, String row, String family)
    throws IOException {
        // Do nothing
    }

    /**
     * @param table
     * @param row
     * @param family
     * @param qualifier
     * @throws IOException
     */
    protected void foundQualifier(HTableInterface table, String row, String family, String qualifier)
    throws IOException {
        // Do nothing
    }

    /**
     * @param table
     * @param row
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    protected void foundValue(HTableInterface table, String row, String family, String qualifier, String value)
    throws IOException {
        // Do nothing
    }

    public boolean isMatch(Level level, String target) {
        Pattern pattern = (Pattern)levelParam.get(level);

        if (pattern == null) {
            return true;
        }

        Matcher matcher = pattern.matcher(target);
        return matcher.find();
    }

    public boolean isGet() {
        return getTaskType() == TaskType.GET;
    }

    public boolean isFilter() {
        return getTaskType() == TaskType.FILTER;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }
}
