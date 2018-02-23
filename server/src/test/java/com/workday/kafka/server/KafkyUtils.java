package com.workday.kafka.server;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by drewwilson on 23/02/2018.
 */
public class KafkyUtils {
    // @todo handle files longer than 2K
    static String getResourceAsString(String name) throws IOException {
        try (InputStream resourceAsStream = KafkyUtils.class.getResourceAsStream(name)) {
            int size = resourceAsStream.available();
            byte[] data = new byte[size];
            resourceAsStream.read(data);
            return new String(data);
        }
    }
}
