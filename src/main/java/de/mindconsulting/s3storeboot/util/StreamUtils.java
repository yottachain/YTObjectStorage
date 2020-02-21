package de.mindconsulting.s3storeboot.util;

import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by phs on 2020/2/19.
 */
public class StreamUtils {
    private static final Logger LOG = Logger.getLogger(StreamUtils.class);
    public static long copy(InputStream in, BufferedOutputStream out) throws IOException {
        long byteCount = 0L;
        byte[] buffer = new byte[131072];
        LOG.info("START WRITE CACHE*****************");
        int bytesRead1;
        for(boolean bytesRead = true; (bytesRead1 = in.read(buffer)) != -1; byteCount += (long)bytesRead1) {
            out.write(buffer, 0, bytesRead1);
        }

        out.flush();
        out.close();
        return byteCount;
    }
}
