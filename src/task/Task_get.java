package task;

import static common.Common.str2bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import main.HBShell;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

import tnode.TNodeBase;
import tnode.TNodeDatabase;
import tnode.TNodeFamily;
import tnode.TNodeFamilyFileData;
import tnode.TNodeQualifier;
import tnode.TNodeRow;
import tnode.TNodeTable;
import utils.Utils;
import exception.HBSException;

public class Task_get extends TaskBase {
    private static final String FAMILY_IS_VALID_BUT_WITHOUT_DATA = "(family is valid but without data)";

    @Override
    protected String description() {
        return "get contents of database, table, row, family or qualifier";
    }

    @Override
    protected String usage() {
        return "get [table_name [row_key [family_name [qualifier_name]]]]";
    }

    @Override
    public String example() {
        return "get 135530186920f18b9049b0a0743e86ac3185887c5d f30dab5e-4b42-11e2-b324-998f21848d86file";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 0 && argNumber <= 4;
    }

    @Override
    public Level getLevel() {
        return Level.VALUE;
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
    public void execute()
    throws IOException, HBSException {
        String table = (String) levelParam.get(Level.TABLE);

        // get database
        if (table == null) {
            getDatabase().handle();
            return;
        }

        String row = (String) levelParam.get(Level.ROW);

        // get table
        if (row == null) {
            getTable(table).handle();
            return;
        }

        String family = (String) levelParam.get(Level.FAMILY);

        // get row
        if (family == null) {
            getRow(table, row).handle();
            return;
        }

        String qualifier = (String) levelParam.get(Level.QUALIFIER);

        // get family
        if (qualifier == null) {
            TNodeRow nRow = getRow(table, row);

            TNodeFamily nFamily = getFamily(row, family, nRow);

            if (nFamily != null) {
                nFamily.handle();
            }

            if (TNodeFamily.isFileDataFamily(family)) {
                getFamilyFileData(family, nRow, nFamily).handle();
            }

            return;
        }

        // get qualifier
        getQualifier(table, row, family, qualifier).handle();
    }

    private TNodeDatabase getDatabase()
    throws HBSException {
        return new TNodeDatabase(this, true);
    }

    private TNodeTable getTable(String table)
    throws HBSException {
        return new TNodeTable(this, null, table, true);
    }

    // see: TNodeTable.travelChildren()
    private TNodeRow getRow(String table, String row)
    throws IOException, HBSException {
        TNodeTable nTable = getTable(table);

        Get get = new Get(str2bytes(row));
        get.setFilter(new FirstKeyOnlyFilter());

        try (HTableInterface hTable = Utils.getTable(table)) {
            Result firstKVResult = hTable.get(get);

            if (firstKVResult.isEmpty()) {
                throw new IOException("row not found '" + row + "'");
            }

            return new TNodeRow(this, nTable, hTable, firstKVResult, true);
        }
    }

    // see: TNodeRow.travelChildrenNonFileData()
    private TNodeFamily getFamily(String row, String family, TNodeRow nRow)
    throws IOException, HBSException {
        Get get = new Get(str2bytes(row));

        // filter family
        get.addFamily(str2bytes(family));

        // filter file data qualifier (excluded)
        get.setFilter(TNodeBase.getFileDataQualifierFilter(false));

        // get result
        Result result = null;

        try {
            result = nRow.table.get(get);
        } catch (NoSuchColumnFamilyException e) {
            // make error clear
            throw new NoSuchColumnFamilyException(family);
        }

        if (result.isEmpty()) {
            if (TNodeFamily.isFileDataFamily(family)) {
                return null;
            }

            throw new NoSuchColumnFamilyException(family + FAMILY_IS_VALID_BUT_WITHOUT_DATA);
        }

        Map<String, Long>    timestampMap   = HBShell.showtimestamp ? Utils.resultGetTimestampMap(result, family) : null;
        Map<String, Integer> valuelengthMap = HBShell.showvaluelength ? Utils.resultGetValueLengthMap(result, family) : null;

        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(str2bytes(family));
        return new TNodeFamily(this, nRow, family, timestampMap, valuelengthMap, familyMap, true);
    }

    // see: TNodeRow.getFamilyFileData()
    private static TNodeFamilyFileData getFamilyFileData(String family, TNodeRow nRow, TNodeFamily nFamily)
    throws IOException, HBSException {
        TNodeFamilyFileData familyFileData = nRow.getFamilyFileData(family, nFamily);

        if (familyFileData == null) {
            throw new NoSuchColumnFamilyException(family + FAMILY_IS_VALID_BUT_WITHOUT_DATA);
        }

        return familyFileData;
    }

    private TNodeBase getQualifier(String table, String row, String family, String qualifier)
    throws IOException, HBSException {
        TNodeRow nRow = getRow(table, row);

        byte[] bRow       = str2bytes(row);
        byte[] bFamily    = str2bytes(family);
        byte[] bQualifier = str2bytes(qualifier);

        Get get = new Get(bRow);

        // filter family & qualifier

        get.addColumn(bFamily, bQualifier);

        // get result
        Result result = null;

        try {
            result = nRow.table.get(get);
        } catch (NoSuchColumnFamilyException e) {
            // make error clear
            throw new NoSuchColumnFamilyException(family);
        }

        if (result.isEmpty()) {
            throw new NoSuchColumnFamilyException(family + ":" + qualifier);
        }

        Map<String, Long> timestampMap = null;
        Long              timestamp    = null;

        if (HBShell.showtimestamp) {
            timestampMap = Utils.resultGetTimestampMap(result, family);
            timestamp    = timestampMap.get(qualifier);
        }

        Map<String, Integer> valuelengthMap = null;
        Integer              valuelength    = null;

        if (HBShell.showvaluelength) {
            valuelengthMap = Utils.resultGetValueLengthMap(result, family);
            valuelength    = valuelengthMap.get(qualifier);
        }

        byte[] bValue = result.getValue(bFamily, bQualifier);

        TNodeFamily nFamily = new TNodeFamily(this, nRow, family, timestampMap, valuelengthMap, result.getFamilyMap(bFamily), true);
        return new TNodeQualifier(this, nFamily, qualifier, timestamp, valuelength, bValue, true);
    }
}
