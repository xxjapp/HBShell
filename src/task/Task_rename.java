package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import utils.Utils;

// see: /home/hadoop/hbase/bin/rename_table.rb
public class Task_rename extends TaskBase {
    @Override
    protected String description() {
        return "rename table in hbase";
    }

    @Override
    protected String usage() {
        return "rename old_table_name new_table_name";
    }

    @Override
    public String example() {
        return "rename test_table test_table2";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 2;
    }

    @Override
    protected void assignParam(String[] args) {
        List<String> tableNames = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            tableNames.add(args[i]);
        }

        levelParam.put(Level.TABLE, tableNames);
    }

    @Override
    public void execute()
    throws IOException {
        List< ? > tableNames = (List< ? >)levelParam.get(Level.TABLE);

        String oldTableName = (String) tableNames.get(0);
        String newTableName = (String) tableNames.get(1);

        try (HBaseAdmin hBaseAdmin = new HBaseAdmin(Utils.conf())) {
            hBaseAdmin.disableTable(oldTableName);
            renameTable(oldTableName, newTableName);
            hBaseAdmin.enableTable(newTableName);
        }
    }

    private static void renameTable(String oldTableName, String newTableName) {
        log.info(String.format("Renaming table(%s -> %s) not supported, however, you can use export, describe, create, import, delete to do the rename task", oldTableName, newTableName));
    }
}
