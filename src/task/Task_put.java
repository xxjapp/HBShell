package task;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

import utils.Utils;

public class Task_put extends TaskBase {
    @Override
    protected String description() {
        return "put a cell 'value' at specified table/row/family:qualifier";
    }

    @Override
    protected String usage() {
        return "put table_name row_key family_name qualifier_name value";
    }

    @Override
    public String example() {
        return "put test_table row1 family1 qualifier1 value1";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 5;
    }

    @Override
    protected void assignParam(String[] args) {
        levelParam.put(Level.TABLE,     args[0]);
        levelParam.put(Level.ROW,       args[1]);
        levelParam.put(Level.FAMILY,    args[2]);
        levelParam.put(Level.QUALIFIER, args[3]);
        levelParam.put(Level.VALUE,     args[4]);
    }

    @Override
    public void execute()
    throws IOException {
        String table     = (String) levelParam.get(Level.TABLE);
        String row       = (String) levelParam.get(Level.ROW);
        String family    = (String) levelParam.get(Level.FAMILY);
        String qualifier = (String) levelParam.get(Level.QUALIFIER);
        String value     = (String) levelParam.get(Level.VALUE);

        try (HTableInterface hTable = Utils.getTable(table)) {
            if (isIncreasePut()) {
                String oldValue = Utils.get(hTable, row, family, qualifier);
                value = String.valueOf(Double.valueOf(oldValue) + Double.valueOf(value));

                if (value.endsWith(".0")) {
                    value = value.substring(0, value.length() - 2);
                }
            }

            if (isAppendPut()) {
                String oldValue = Utils.get(hTable, row, family, qualifier);
                value = oldValue + value;
            }

            Utils.put(hTable, row, family, qualifier, value);
        } catch (NoSuchColumnFamilyException e) {
            // make error clear
            throw new NoSuchColumnFamilyException(family);
        }
    }
}
