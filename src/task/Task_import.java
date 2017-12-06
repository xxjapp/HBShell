package task;

import static common.Common.decode;
import static common.Common.encode;

import java.io.File;
import java.io.IOException;

import main.HBShell;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTableInterface;

import utils.Utils;

public class Task_import extends TaskBase {
    @Override
    protected String description() {
        return "import binary contents of database, table, row, family or qualifier into database";
    }

    @Override
    protected String usage() {
        return "import [table_name [row_key [family_name [qualifier_name]]]]";
    }

    @Override
    public String example() {
        return "import 135530186920f18b9049b0a0743e86ac3185887c5d f30dab5e-4b42-11e2-b324-998f21848d86file";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 0 && argNumber <= 4;
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
    throws IOException {
        String table = (String) levelParam.get(Level.TABLE);

        // get database
        if (table == null) {
            importDatabase();
            return;
        }

        try (HTableInterface hTable = Utils.getTable(table)) {
            String row = (String) levelParam.get(Level.ROW);

            // get table
            if (row == null) {
                importTable(hTable, table);
                return;
            }

            String family = (String) levelParam.get(Level.FAMILY);

            // get row
            if (family == null) {
                importRow(hTable, table, row);
                return;
            }

            String qualifier = (String) levelParam.get(Level.QUALIFIER);

            // get family
            if (qualifier == null) {
                importFamily(hTable, table, row, family);
                return;
            }

            // get qualifier
            importQualifier(hTable, table, row, family, qualifier);
        }
    }

    private static void importQualifier(HTableInterface hTable, String table, String row, String family, String qualifier)
    throws IOException {
        String t = encode(table);
        String r = encode(row);
        String f = encode(family);
        String q = encode(qualifier);

        String valueFilePath = String.format("%s/%s/%s/%s/%s.txt", HBShell.binaryDataDir, t, r, f, q);
        byte[] bValue        = FileUtils.readFileToByteArray(new File(valueFilePath));

        Utils.put(hTable, row, family, qualifier, bValue);
    }

    private static void importFamily(HTableInterface hTable, String table, String row, String family)
    throws IOException {
        String t = encode(table);
        String r = encode(row);
        String f = encode(family);

        String   familyDirPath = String.format("%s/%s/%s/%s", HBShell.binaryDataDir, t, r, f);
        File     familyDir     = new File(familyDirPath);
        String[] qs            = familyDir.list();

        for (String q : qs) {
            String qualifier = decode(q.substring(0, q.length() - ".txt".length()));
            importQualifier(hTable, table, row, family, qualifier);
        }
    }

    private static void importRow(HTableInterface hTable, String table, String row)
    throws IOException {
        String t = encode(table);
        String r = encode(row);

        String   rowDirPath = String.format("%s/%s/%s", HBShell.binaryDataDir, t, r);
        File     rowDir     = new File(rowDirPath);
        String[] fs         = rowDir.list();

        for (String f : fs) {
            String family = decode(f);
            importFamily(hTable, table, row, family);
        }
    }

    private static void importTable(HTableInterface hTable, String table)
    throws IOException {
        String t = encode(table);

        String   tableDirPath = String.format("%s/%s", HBShell.binaryDataDir, t);
        File     tableDir     = new File(tableDirPath);
        String[] rs           = tableDir.list();

        for (String r : rs) {
            String row = decode(r);
            importRow(hTable, table, row);
        }
    }

    private static void importDatabase()
    throws IOException {
        String   dbDirPath = String.format("%s", HBShell.binaryDataDir);
        File     dbDir     = new File(dbDirPath);
        String[] ts        = dbDir.list();

        for (String t : ts) {
            String table = decode(t);

            try (HTableInterface hTable = Utils.getTable(table)) {
                importTable(hTable, table);
            }
        }
    }
}
