package task;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;

import exception.HBSException;
import utils.Utils;

public class Task_delete extends TaskBase {
    @Override
    protected String description() {
        return "delete data in database\n" +
               "\n" +
               "** WARNING : 'delete'(and its alias) will delete all tables in database\n" +
               "** NOTE    : use 'delete! ...' to force delete";
    }

    @Override
    protected String usage() {
        return "delete [table_name [row_key [family_name [qualifier_name]]]]";
    }

    @Override
    public String example() {
        return "delete test_table family1";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 0 <= argNumber && argNumber <= 4;
    }

    @Override
    protected Level getLevel() {
        if (levelParam.size() > 0) {
            return Level.values()[levelParam.size() - 1];
        }

        return Level.TABLE;
    }

    @Override
    protected boolean needConfirm() {
        return true;
    }

    @Override
    protected void assignParam(String[] args) {
        try {
            levelParam.put(Level.TABLE,     args[0]);
            levelParam.put(Level.ROW,       args[1]);
            levelParam.put(Level.FAMILY,    args[2]);
            levelParam.put(Level.QUALIFIER, args[3]);
        } catch (ArrayIndexOutOfBoundsException e) {
            // OK
        }
    }

    @Override
    public void confirm()
    throws IOException, HBSException {
        Task_get task_get = new Task_get();

        task_get.level      = task_get.getLevel();
        task_get.levelParam = levelParam;

        task_get.execute();
    }

    @Override
    public void resetAllCount() {
        // keep all count becuase there is no counter for deleting
    }

    @Override
    public void execute()
    throws IOException, HBSException {
        String table = (String) levelParam.get(Level.TABLE);

        // delete all tables in database
        if (table == null) {
            deleteDatabase();
            return;
        }

        String row = (String) levelParam.get(Level.ROW);

        // delete table
        if (row == null) {
            deleteTable(table);
            return;
        }

        String family = (String) levelParam.get(Level.FAMILY);

        // delete row
        if (family == null) {
            deleteRow(table, row);
            return;
        }

        String qualifier = (String) levelParam.get(Level.QUALIFIER);

        // delete family
        if (qualifier == null) {
            deleteFamily(table, row, family);
            return;
        }

        // delete qualifier
        deleteQualifier(table, row, family, qualifier);
    }

    private void deleteDatabase()
    throws IOException {
        HTableDescriptor[] hTableDescriptors = Utils.listTables();

        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            String table = hTableDescriptor.getNameAsString();
            Utils.deleteTable(table);
        }
    }

    private void deleteTable(String table)
    throws IOException {
        Utils.deleteTable(table);
    }

    private void deleteRow(String table, String row)
    throws IOException {
        HTable hTable = Utils.getTable(table);

        try {
            Utils.deleteRow(hTable, row);
        } finally {
            hTable.close();
        }
    }

    private void deleteFamily(String table, String row, String family)
    throws IOException {
        HTable hTable = Utils.getTable(table);

        try {
            Utils.deleteFamily(hTable, row, family);
        } finally {
            hTable.close();
        }
    }

    private void deleteQualifier(String table, String row, String family, String qualifier)
    throws IOException {
        HTable hTable = Utils.getTable(table);

        try {
            Utils.deleteQualifier(hTable, row, family, qualifier);
        } finally {
            hTable.close();
        }
    }
}
