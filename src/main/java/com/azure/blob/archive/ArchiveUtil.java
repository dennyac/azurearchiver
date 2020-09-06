package com.azure.blob.archive;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.BlockBlobCommitBlockListResponse;
import io.reactivex.Flowable;
import io.reactivex.Observable;

public class ArchiveUtil {

    private final String containerName;
    private final String accountName;
    private final String accountKey;
    private final String storageURL;
    private final String protocolScheme;

    public ArchiveUtil(String accountName,
                       String accountKey,
                       String protocolScheme,
                       String storageURL,
                       String containerName) throws MalformedURLException {
        this.accountName = accountName;
        this.accountKey = accountKey;
        this.protocolScheme = protocolScheme;
        this.storageURL = storageURL;
        this.containerName = containerName;
    }

    public String uploadBlock(byte[] byteArray, String blobFileName) throws MalformedURLException, InvalidKeyException {
        final BlockBlobURL blockBlobURL = getContainerURL().createBlockBlobURL(blobFileName);
        long blockLength = BlockBlobURL.MAX_STAGE_BLOCK_BYTES;
        TransferManagerUploadToBlockBlobOptions optionsReal = TransferManagerUploadToBlockBlobOptions.DEFAULT;
        String blockId = Base64.getEncoder().encodeToString(
                UUID.randomUUID().toString().getBytes());
        blockBlobURL.stageBlock(blockId, Flowable.just(ByteBuffer.wrap(byteArray)),
                byteArray.length, optionsReal.accessConditions().leaseAccessConditions(), null)
                .blockingGet();
        return blockId;
    }

    public BlockBlobCommitBlockListResponse createArchive(List<Map.Entry<Integer, Flowable<ByteBuffer>>> flowableList, final String blobFileName) throws MalformedURLException, InvalidKeyException {
        final BlockBlobURL blockBlobURL = getContainerURL().createBlockBlobURL(blobFileName);

        long blockLength = BlockBlobURL.MAX_STAGE_BLOCK_BYTES;
        TransferManagerUploadToBlockBlobOptions optionsReal = TransferManagerUploadToBlockBlobOptions.DEFAULT;
        int numBlocks = flowableList.size();

        return Observable.range(0, numBlocks)
                /*
                For each block, make a call to stageBlock as follows. concatMap ensures that the items emitted
                by this Observable are in the same sequence as they are begun, which will be important for composing
                the list of Ids later. Eager ensures parallelism but may require some internal buffering.
                 */
                .concatMapEager(i -> {
                    // The max number of bytes for a block is currently 100MB, so the final result must be an int.
                    //int count = (int) Math.min((long)blockLength, (file.size() - i * (long)blockLength));
                    int count = flowableList.get(i).getKey();
                    // i * blockLength could be a long, so we need a cast to prevent overflow.
                    Flowable<ByteBuffer> data = flowableList.get(i).getValue();

//                    // Report progress as necessary.
//                    data = ProgressReporter.addParallelProgressReporting(data, optionsReal.progressReceiver(),
//                            progressLock, totalProgress);

                    final String blockId = Base64.getEncoder().encodeToString(
                            UUID.randomUUID().toString().getBytes());

                    /*
                    Make a call to stageBlock. Instead of emitting the response, which we don't care about other
                    than that it was successful, emit the blockId for this request. These will be collected below.
                    Turn that into an Observable which emits one item to comply with the signature of
                    concatMapEager.
                     */
                    return blockBlobURL.stageBlock(blockId, data,
                            count, optionsReal.accessConditions().leaseAccessConditions(), null)
                            .map(x -> blockId).toObservable();

                    /*
                    Specify the number of concurrent subscribers to this map. This determines how many concurrent
                    rest calls are made. This is so because maxConcurrency is the number of internal subscribers
                    available to subscribe to the Observables emitted by the source. A subscriber is not released
                    for a new subscription until its Observable calls onComplete, which here means that the call to
                    stageBlock is finished. Prefetch is a hint that each of the Observables emitted by the source
                    will emit only one value, which is true here because we have converted from a Single.
                     */
                }, optionsReal.parallelism(), 1)
                /*
                collectInto will gather each of the emitted blockIds into a list. Because we used concatMap, the Ids
                will be emitted according to their block number, which means the list generated here will be
                properly ordered. This also converts into a Single.
                 */
                .collectInto(new ArrayList<String>(numBlocks), ArrayList::add)
                /*
                collectInto will not emit the list until its source calls onComplete. This means that by the time we
                call stageBlock list, all of the stageBlock calls will have finished. By flatMapping the list, we
                can "map" it into a call to commitBlockList.
                */
                .flatMap(ids ->
                        blockBlobURL.commitBlockList(ids, optionsReal.httpHeaders(), optionsReal.metadata(),
                                optionsReal.accessConditions(), null)).blockingGet();
    }
    public BlockBlobCommitBlockListResponse createArchiveFromBlobs(List<BlockBlobURL> blockBlobURLList, final String blobFileName) throws MalformedURLException, InvalidKeyException {
        return createArchiveFromUrls(blockBlobURLList.stream().map(x -> x.toURL()).collect(Collectors.toList()), blobFileName);
    }

    public String uploadBlockFromUrl(URL sasURL, String blobName) throws MalformedURLException, InvalidKeyException {
        final BlockBlobURL blockBlobURL = getContainerURL().createBlockBlobURL(blobName);
        TransferManagerUploadToBlockBlobOptions optionsReal = TransferManagerUploadToBlockBlobOptions.DEFAULT;
        final String blockId = Base64.getEncoder().encodeToString(
                UUID.randomUUID().toString().getBytes());
        blockBlobURL.stageBlockFromURL(blockId, sasURL, null).blockingGet();
        return blockId;
    }

    public void commitBlockList(List<String> blockList, String blockName) throws MalformedURLException, InvalidKeyException {
        final BlockBlobURL blockBlobURL = getContainerURL().createBlockBlobURL(blockName);
        TransferManagerUploadToBlockBlobOptions optionsReal = TransferManagerUploadToBlockBlobOptions.DEFAULT;

        blockBlobURL.commitBlockList(blockList, optionsReal.httpHeaders(), optionsReal.metadata(),
                optionsReal.accessConditions(), null).blockingGet();
    }


    public BlockBlobCommitBlockListResponse createArchiveFromUrls(List<URL> blockBlobURLList, final String blobFileName) throws MalformedURLException, InvalidKeyException {
        final BlockBlobURL blockBlobURL = getContainerURL().createBlockBlobURL(blobFileName);

        long blockLength = BlockBlobURL.MAX_STAGE_BLOCK_BYTES;
        TransferManagerUploadToBlockBlobOptions optionsReal = TransferManagerUploadToBlockBlobOptions.DEFAULT;
        int numBlocks = blockBlobURLList.size();

        return Observable.range(0, numBlocks)
                /*
                For each block, make a call to stageBlock as follows. concatMap ensures that the items emitted
                by this Observable are in the same sequence as they are begun, which will be important for composing
                the list of Ids later. Eager ensures parallelism but may require some internal buffering.
                 */
                .concatMapEager(i -> {
                    // The max number of bytes for a block is currently 100MB, so the final result must be an int.
                    //int count = (int) Math.min((long)blockLength, (file.size() - i * (long)blockLength));
                    //int count = flowableList.get(i).getKey();
                    // i * blockLength could be a long, so we need a cast to prevent overflow.
                    // Flowable<ByteBuffer> data = flowableList.get(i).getValue();

//                    // Report progress as necessary.
//                    data = ProgressReporter.addParallelProgressReporting(data, optionsReal.progressReceiver(),
//                            progressLock, totalProgress);

                    final String blockId = Base64.getEncoder().encodeToString(
                            UUID.randomUUID().toString().getBytes());

                    /*
                    Make a call to stageBlock. Instead of emitting the response, which we don't care about other
                    than that it was successful, emit the blockId for this request. These will be collected below.
                    Turn that into an Observable which emits one item to comply with the signature of
                    concatMapEager.


                     */
                    return blockBlobURL.stageBlockFromURL(blockId, blockBlobURLList.get(i), null).map(x -> blockId).toObservable();
//                    return blockBlobURL.stageBlock(blockId, data,
//                            count, optionsReal.accessConditions().leaseAccessConditions(), null)
//                            .map(x -> blockId).toObservable();

                    /*
                    Specify the number of concurrent subscribers to this map. This determines how many concurrent
                    rest calls are made. This is so because maxConcurrency is the number of internal subscribers
                    available to subscribe to the Observables emitted by the source. A subscriber is not released
                    for a new subscription until its Observable calls onComplete, which here means that the call to
                    stageBlock is finished. Prefetch is a hint that each of the Observables emitted by the source
                    will emit only one value, which is true here because we have converted from a Single.
                     */
                }, optionsReal.parallelism(), 1)
                /*
                collectInto will gather each of the emitted blockIds into a list. Because we used concatMap, the Ids
                will be emitted according to their block number, which means the list generated here will be
                properly ordered. This also converts into a Single.
                 */
                .collectInto(new ArrayList<String>(numBlocks), ArrayList::add)
                /*
                collectInto will not emit the list until its source calls onComplete. This means that by the time we
                call stageBlock list, all of the stageBlock calls will have finished. By flatMapping the list, we
                can "map" it into a call to commitBlockList.
                */
                .flatMap(ids ->
                        blockBlobURL.commitBlockList(ids, optionsReal.httpHeaders(), optionsReal.metadata(),
                                optionsReal.accessConditions(), null)).blockingGet();
    }

    private SharedKeyCredentials getCredentials() throws InvalidKeyException {
        return new SharedKeyCredentials(accountName, accountKey);
    }

    public BlockBlobURL getBlockBlobURL(String blockName) throws MalformedURLException, InvalidKeyException {
        return getContainerURL().createBlockBlobURL(blockName);
    }

    private ContainerURL getContainerURL() throws MalformedURLException, InvalidKeyException {
        ServiceURL serviceURL = new ServiceURL(new URL(String.format("%s://%s.%s", protocolScheme,
                accountName, storageURL)),
                StorageURL.createPipeline(getCredentials(), new PipelineOptions()));
        return serviceURL.createContainerURL(containerName);
    }
}

