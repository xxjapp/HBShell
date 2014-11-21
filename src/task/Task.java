package task;

import java.io.IOException;
import java.util.List;

import task.TaskBase.Level;

public interface Task {
    void doTask(String[] args)
    throws IOException;

    void printHelp();

    List< ? > alias();

    String example();

    boolean isReadOnly();

    boolean outpuBinary();

    boolean isHandleAll();

    boolean isIncreasePut();

    boolean isAppendPut();

    long getRowLimit();

    void changeLogOnStart();

    Level getLevel();

    boolean needConfirm();

    boolean notifyEnabled();

    boolean isToOutput();

    void resetAllCount();

    void cancel();
}
