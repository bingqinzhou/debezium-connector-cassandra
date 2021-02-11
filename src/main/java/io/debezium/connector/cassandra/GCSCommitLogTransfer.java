/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableSet;

public class GCSCommitLogTransfer implements CommitLogTransfer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSCommitLogTransfer.class);
    // see: https://cloud.google.com/storage/docs/resumable-uploads-xml#practices
    private static final Set<Integer> RETRYABLE_ERRORS = ImmutableSet.of(408, 429, 500, 502, 503, 504);
    private static final int MAX_ATTEMPTS = 10;
    private static final String FILE_CONTENT_TYPE = "text/plain";
    private static final int MAX_BACKOFF_SECONDS = 2 * 60 * 60; // 2 hours
    private static final String GCS_PREFIX = "gs://";

    private static final String STORAGE_CREDENTIAL_KEY_FILE = "storage.credential.key.file";
    private static final String REMOTE_COMMIT_LOG_RELOCATION_DIR = "remote.commit.log.relocation.dir";

    private Storage storage;
    private String bucket;
    private String prefix;
    private File cdcDir;

    @Override
    public void init(Properties commitLogTransferConfigs) throws IOException {
        storage = getStorage(commitLogTransferConfigs.getProperty(STORAGE_CREDENTIAL_KEY_FILE));
        bucket = getBucket(commitLogTransferConfigs.getProperty(REMOTE_COMMIT_LOG_RELOCATION_DIR));
        prefix = getPrefix(commitLogTransferConfigs.getProperty(REMOTE_COMMIT_LOG_RELOCATION_DIR));
        cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
    }

    @Override
    public void onSuccessTransfer(File file) {
        String archivePrefix = Paths.get(prefix, QueueProcessor.ARCHIVE_FOLDER).toString();
        if (uploadWithRetry(file, bucket, archivePrefix)) {
            CommitLogUtil.deleteCommitLog(file);
        }
    }

    @Override
    public void onErrorTransfer(File file) {
        String errorPrefix = Paths.get(prefix, QueueProcessor.ERROR_FOLDER).toString();
        if (uploadWithRetry(file, bucket, errorPrefix)) {
            CommitLogUtil.deleteCommitLog(file);
        }
    }

    @Override
    public void getErrorCommitLogFiles() {
        String errorPrefix = Paths.get(prefix, QueueProcessor.ERROR_FOLDER).toString();
        Page<Blob> blobs = listWithRetry(bucket, errorPrefix);
        if (blobs != null) {
            int cnt = 0;
            for (Blob blob : blobs.iterateAll()) {
                cnt++;
                if (downloadWithRetry(blob)) {
                    deleteWithRetry(blob);
                }
            }
            if (cnt == 1) {
                LOGGER.info("No error CommitLog files found in {}", Paths.get(bucket, errorPrefix));
            }
        }
    }

    Storage getStorage(String keyFile) throws IOException {
        StorageOptions options = keyFile != null
                ? StorageOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(keyFile))).build()
                : StorageOptions.newBuilder().setCredentials(ServiceAccountCredentials.getApplicationDefault()).build();
        return options.getService();
    }

    static String getBucket(String remoteCommitLogDir) {
        String[] bucketAndPrefix = remoteCommitLogDir.substring(GCS_PREFIX.length()).split("/", 2);
        return bucketAndPrefix[0];
    }

    static String getPrefix(String remoteCommitLogDir) {
        String[] bucketAndPrefix = remoteCommitLogDir.substring(GCS_PREFIX.length()).split("/", 2);
        return bucketAndPrefix[1];
    }

    boolean uploadWithRetry(File file, String bucket, String prefix) {
        if (!file.exists()) {
            LOGGER.warn("Unable to upload file {} since it doesn't exist.", file.getPath());
            return false;
        }
        ExponentialBackOff backOff = new ExponentialBackOff(MAX_BACKOFF_SECONDS);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
            LOGGER.info("Attempting to upload file {}, try number {}", file.getName(), attempt + 1);
            try {
                uploadFile(file, bucket, prefix);
                LOGGER.info("Successfully uploaded file to url gs://{}/{}/{}", bucket, prefix, file.getName());
                return true;
            }
            catch (Exception e) {
                if (isRetryable(e)) {
                    LOGGER.warn("Uploading {} failed, retrying in {}s", file.getName(), backOff.getBackoffMs() / 1000);
                    try {
                        backOff.doWait();
                    }
                    catch (InterruptedException ie) {
                        LOGGER.error("Thread was interrupted before the following file could be retried: " + file.getName(), ie);
                        return false;
                    }
                }
                else {
                    LOGGER.error("Could not upload file {}: ", file.getName(), e);
                    return false;
                }
            }
        }
        LOGGER.error("Exhausted attempts to upload {} after {} tries", file.getName(), MAX_ATTEMPTS);
        return false;
    }

    private void uploadFile(File file, String bucket, String prefix) throws IOException {
        byte[] content = Files.readAllBytes(file.toPath());
        String fullPathFileName = Paths.get(prefix, file.getName()).toString();
        BlobId blobId = BlobId.of(bucket, fullPathFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(FILE_CONTENT_TYPE).build();
        storage.create(blobInfo, content);
    }

    boolean downloadWithRetry(Blob blob) {
        ExponentialBackOff backOff = new ExponentialBackOff(MAX_BACKOFF_SECONDS);
        String commitLogFileName = blob.getBlobId().getName().split("/", 3)[2];
        if (commitLogFileName.equals("")) {
            // Avoid trying to download the prefix folder, something like gs://u_bingqinz/cassandra_commit_logs/error.
            return false;
        }
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
            LOGGER.info("Attempting to download file {}, try number {}", blob.getBlobId().getName(), attempt + 1);
            try {
                // Please refer to: https://cloud.google.com/storage/docs/downloading-objects
                blob.downloadTo(Paths.get(cdcDir.toPath().toString(), commitLogFileName));
                LOGGER.info("Successfully download file {}", blob.getBlobId().getName());
                return true;
            }
            catch (Exception e) {
                if (isRetryable(e)) {
                    LOGGER.warn("Trying to download file {} failed, retrying in {}s.", blob.getBlobId().getName(), backOff.getBackoffMs() / 1000);
                    try {
                        backOff.doWait();
                    }
                    catch (InterruptedException ie) {
                        LOGGER.error("Thread was interrupted before retrying to download file {}: ", blob.getBlobId().getName(), ie);
                        return false;
                    }
                }
                else {
                    LOGGER.error("Could not download file {}: ", blob.getBlobId().getName(), e);
                    return false;
                }
            }
        }
        LOGGER.error("Exhausted attempts to download file {} after {} tries", blob.getBlobId().getName(), MAX_ATTEMPTS);
        return false;
    }

    boolean deleteWithRetry(Blob blob) {
        ExponentialBackOff backOff = new ExponentialBackOff(MAX_BACKOFF_SECONDS);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
            LOGGER.info("Attempting to delete file {}, try number {}", blob.getBlobId().getName(), attempt + 1);
            try {
                // Please refer to: https://cloud.google.com/storage/docs/deleting-objects#storage-delete-object-java
                storage.delete(blob.getBlobId());
                LOGGER.info("Successfully delete file {}", blob.getBlobId().getName());
                return true;
            }
            catch (Exception e) {
                if (isRetryable(e)) {
                    LOGGER.warn("Trying to delete file {} failed, retrying in {}s.", blob.getBlobId().getName(), backOff.getBackoffMs() / 1000);
                    try {
                        backOff.doWait();
                    }
                    catch (InterruptedException ie) {
                        LOGGER.error("Thread was interrupted before retrying to delete file {}: ", blob.getBlobId().getName(), ie);
                        return false;
                    }
                }
                else {
                    LOGGER.error("Could not delete file {}: ", blob.getBlobId().getName(), e);
                    return false;
                }
            }
        }
        LOGGER.error("Exhausted attempts to delete file {} after {} tries", blob.getBlobId().getName(), MAX_ATTEMPTS);
        return false;
    }

    Page<Blob> listWithRetry(String bucket, String prefix) {
        ExponentialBackOff backOff = new ExponentialBackOff(MAX_BACKOFF_SECONDS);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
            LOGGER.info("Attempting to get files in gs://{}/{}, try number {}", bucket, prefix, attempt + 1);
            try {
                LOGGER.info("Successfully get files in gs://{}/{}", bucket, prefix);
                // Please refer to: https://cloud.google.com/storage/docs/listing-objects
                return storage.get(bucket).list(
                        Storage.BlobListOption.prefix(prefix));
            }
            catch (Exception e) {
                if (isRetryable(e)) {
                    LOGGER.warn("Getting files in gs://{}/{} failed, retrying in {}s.", bucket, prefix, backOff.getBackoffMs() / 1000);
                    try {
                        backOff.doWait();
                    }
                    catch (InterruptedException ie) {
                        LOGGER.error("Thread was interrupted before getting files in gs://{}/{} could be retried: ", bucket, prefix, ie);
                        return null;
                    }
                }
                else {
                    LOGGER.error("Could not get files in gs://{}/{}: ", bucket, prefix, e);
                    return null;
                }
            }
        }
        LOGGER.error("Exhausted attempts to get files in gs://{}/{} after {} tries", bucket, prefix, MAX_ATTEMPTS);
        return null;
    }

    private static boolean isRetryable(Exception e) {
        if (e instanceof StorageException) {
            Throwable cause = e.getCause();
            if (cause instanceof GoogleJsonResponseException) {
                GoogleJsonResponseException inner = (GoogleJsonResponseException) cause;
                if (inner.getDetails() != null) {
                    int code = inner.getDetails().getCode();
                    return RETRYABLE_ERRORS.contains(code);
                }
            }
        }
        return false;
    }
}
