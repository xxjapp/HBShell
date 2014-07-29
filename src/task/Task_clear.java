package task;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;

import utils.Utils;

public class Task_clear extends TaskBase {
    @Override
    protected String description() {
        return "clear table contents\n" +
               "\n" +
               "    ** WARNING : 'clear'(and its alias) will clear contents of all tables in database\n" +
               "    ** NOTE    : use 'clear! ...' to force clear";
    }

    @Override
    protected String usage() {
        return "clear [table_pattern]";
    }

    @Override
    public String example() {
        return "clear ^test_table";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0 || argNumber == 1;
    }

    @Override
    public Level getLevel() {
        return Level.TABLE;
    }

    @Override
    public boolean needConfirm() {
        return true;
    }

    @Override
    public boolean notifyEnabled() {
        return true;
    }

    @Override
    protected void foundTable(HTable table)
    throws IOException {
        String       tableName = Utils.bytes2str(table.getTableName());
        List<String> families  = Utils.getFamilies(table);

        Utils.deleteTable(tableName);
        Utils.createTable(tableName, families);
    }
}
