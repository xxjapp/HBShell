package task;

public class Task_export extends Task_get {
    @Override
    protected String description() {
        return "export binary contents of database, table, row, family or qualifier";
    }

    @Override
    protected String usage() {
        return "export [table_name [row_key [family_name [qualifier_name]]]]";
    }

    @Override
    public String example() {
        return "export 135530186920f18b9049b0a0743e86ac3185887c5d f30dab5e-4b42-11e2-b324-998f21848d86file";
    }

    @Override
    public boolean outpuBinary() {
        return true;
    }

    @Override
    public boolean isHandleAll() {
        return true;
    }
}
