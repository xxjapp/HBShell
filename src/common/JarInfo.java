package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class JarInfo {
    private static final String UTF_8         = "UTF-8";
    private static final String LIB_DIR       = "lib";
    private static final String JAR_LIST_FILE = LIB_DIR + "/" + "_jar_list.txt";

    public static void checkJarFiles()
    throws IOException {
        String         jarListFile    = JAR_LIST_FILE;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(jarListFile), UTF_8));
        String         line           = null;

        try {
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    // skip comments
                    continue;
                }

                if (line.length() == 0) {
                    // skip empty lines
                    continue;
                }

                String jarFilePath = LIB_DIR + "/" + line;

                if (!new File(jarFilePath).exists()) {
                    throw new FileNotFoundException(jarFilePath);
                }
            }
        } finally {
            bufferedReader.close();
        }
    }
}
