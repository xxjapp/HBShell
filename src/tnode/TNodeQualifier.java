package tnode;

import java.io.IOException;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifier extends TNodeBase {
    private final byte[] bValue;

    public TNodeQualifier(TaskBase task, TNodeFamily parent, String qualifier, byte[] bValue, boolean toOutput) {
        super(task, parent, qualifier, Level.QUALIFIER, toOutput);

        this.bValue = bValue;
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifier;
    }

    @Override
    public void output()
    throws IOException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.QUALIFIER);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException {
        new TNodeValue(task, this, bValue, toOutput).handle();
    }
}
