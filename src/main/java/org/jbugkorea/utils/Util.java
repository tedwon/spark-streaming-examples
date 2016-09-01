package org.jbugkorea.utils;

import java.io.*;

public class Util {
    public static String readResource(InputStream is) throws IOException {
        try {
            final Reader reader = new InputStreamReader(is, "UTF-8");
            StringWriter writer = new StringWriter();
            char[] buf = new char[1024];
            int len;
            while ((len = reader.read(buf)) != -1) {
                writer.write(buf, 0, len);
            }
            return writer.toString();
        } finally {
            is.close();
        }
    }
}
