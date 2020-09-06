package com.azure.blob.archive;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.CRC32;

/**
 * Created by deabrah on 4/27/19.
 */
public class FileUtil {
    private static CRC32 getCRC(File file) throws IOException {
        CRC32 c = new CRC32();
        c.update(Files.readAllBytes(file.toPath()));
        return c;
    }

    private static long getSize(File file) {
        return file.length();
    }

    public static ZipEntry getZipEntry(File file) throws IOException {
        ZipEntry zipEntry = new ZipEntry(file.getName());
        zipEntry.crc = getCRC(file).getValue();
        Long zipEntrySize = getSize(file);
        zipEntry.setSize(zipEntrySize);
        zipEntry.setCompressedSize(zipEntrySize);
        return zipEntry;
    }
}
