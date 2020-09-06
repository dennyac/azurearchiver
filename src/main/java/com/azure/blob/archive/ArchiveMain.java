package com.azure.blob.archive;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

public class ArchiveMain {
    private static final String ACCOUNT_NAME = "ACCOUNT_NAME";
    private static final String AZURE_BLOB_ACCOUNT_KEY = "AZURE_BLOB_ACCOUNT_KEY";
    private static final String PROTOCOL_SCHEME = "https";
    private static final String STORAGE_URL = "blob.core.windows.net";
    private static final String CONTAINER_NAME = "archive";
    private static final String FILE_ONE_LOCAL_PATH = "FILE_ONE_LOCAL_PATH";
    private static final String FILE_ONE_SAS_URL = "https://ACCOUNT_NAME.blob.core.windows.net/archive/FILE_ONE.jpg";
    private static final String FILE_TWO_LOCAL_PATH = "FILE_TWO_LOCAL_PATH";
    private static final String FILE_TWO_SAS_URL = "https://ACCOUNT_NAME.blob.core.windows.net/archive/FILE_TWO.jpg";
    private static final String ARCHIVE_NAME = "Archive.zip";


    public static void main(String[] args) throws IOException {
        ArchiveUtil azureBlobStorage = new ArchiveUtil(ACCOUNT_NAME,
                AZURE_BLOB_ACCOUNT_KEY,
                PROTOCOL_SCHEME,
                STORAGE_URL,
                CONTAINER_NAME);

        AzureArchiver azureArchiver  = new AzureArchiver(new ByteArrayOutputStream(), azureBlobStorage,ARCHIVE_NAME );
        azureArchiver.putNextEntry(FileUtil.getZipEntry(new File(FILE_ONE_LOCAL_PATH)));
        azureArchiver.uploadFromURL(new URL(FILE_ONE_SAS_URL));
        azureArchiver.putNextEntry(FileUtil.getZipEntry(new File(FILE_TWO_LOCAL_PATH)));
        azureArchiver.uploadFromURL(new URL(FILE_TWO_SAS_URL));
        azureArchiver.close();

    }
}
