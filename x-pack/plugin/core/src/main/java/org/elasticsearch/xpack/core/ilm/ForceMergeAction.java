/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link LifecycleAction} which force-merges the index.
 */
public class ForceMergeAction implements LifecycleAction {
    private static final Logger logger = LogManager.getLogger(ForceMergeAction.class);

    private static final Settings BEST_COMPRESSION_SETTINGS = Settings.builder()
        .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
        .build();
    private static final Settings CLONE_SETTINGS_WITHOUT_CODEC = Settings.builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .build();
    private static final Function<IndexMetadata, Settings> CLONE_SETTINGS_WITHOUT_CODEC_SUPPLIER =
        indexMetadata -> CLONE_SETTINGS_WITHOUT_CODEC;
    private static final Settings CLONE_SETTINGS_WITH_CODEC = Settings.builder()
        .put(CLONE_SETTINGS_WITHOUT_CODEC)
        .put(BEST_COMPRESSION_SETTINGS)
        .build();
    private static final Function<IndexMetadata, Settings> CLONE_SETTINGS_WITH_CODEC_SUPPLIER = indexMetadata -> CLONE_SETTINGS_WITH_CODEC;

    public static final String NAME = "forcemerge";
    public static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");
    public static final ParseField CODEC = new ParseField("index_codec");

    public static final String FORCE_MERGE_INDEX_PREFIX = "force-merge-";
    public static final BiFunction<String, LifecycleExecutionState, String> FORCE_MERGE_INDEX_NAME_SUPPLIER = (indexName, state) -> state
        .forceMergeIndexName();

    public static final String CONDITIONAL_SKIP_FORCE_MERGE_STEP = BranchingStep.NAME + "-forcemerge-check-prerequisites";
    public static final String CONDITIONAL_SKIP_CLONE_STEP = BranchingStep.NAME + "-skip-clone-check";
    public static final String UPDATE_COMPRESSION_SETTINGS_STEP = UpdateSettingsStep.NAME + "-compression-settings";
    public static final String WAIT_FOR_COMPRESSION_SETTINGS_GREEN = WaitForIndexColorStep.NAME + "-compression-settings";
    public static final String WAIT_FOR_CLONED_INDEX_GREEN = WaitForIndexColorStep.NAME + "-cloned-index";
    public static final String CONDITIONAL_CONFIGURE_CLONED_INDEX_STEP = BranchingStep.NAME + "-configure-cloned-index-check";
    public static final String UPDATE_CLONED_INDEX_SETTINGS_STEP = UpdateSettingsStep.NAME + "-cloned-index";
    public static final String CONDITIONAL_DATA_STREAM_CHECK_STEP = BranchingStep.NAME + "-on-data-stream-check";

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME, false, a -> {
        int maxNumSegments = (int) a[0];
        String codec = a[1] != null ? (String) a[1] : null;
        return new ForceMergeAction(maxNumSegments, codec);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CODEC);
    }

    private final int maxNumSegments;
    private final String codec;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments, @Nullable String codec) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName() + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        if (codec != null && CodecService.BEST_COMPRESSION_CODEC.equals(codec) == false) {
            throw new IllegalArgumentException("unknown index codec: [" + codec + "]");
        }
        this.codec = codec;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
        this.codec = in.readOptionalString();
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public String getCodec() {
        return this.codec;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
        out.writeOptionalString(codec);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        if (codec != null) {
            builder.field(CODEC.getPreferredName(), codec);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        final boolean codecChange = codec != null && codec.equals(CodecService.BEST_COMPRESSION_CODEC);

        StepKey conditionalSkipForceMergeKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_FORCE_MERGE_STEP);
        StepKey checkNotWriteIndexKey = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitUntilTimeSeriesEndTimeKey = new StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);

        StepKey conditionalSkipCloneKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_CLONE_STEP);

        StepKey closeIndexKey = new StepKey(phase, NAME, CloseIndexStep.NAME);
        StepKey updateBestCompressionKey = new StepKey(phase, NAME, UPDATE_COMPRESSION_SETTINGS_STEP);
        StepKey openIndexKey = new StepKey(phase, NAME, OpenIndexStep.NAME);
        StepKey waitForGreenIndexKey = new StepKey(phase, NAME, WAIT_FOR_COMPRESSION_SETTINGS_GREEN);

        StepKey cleanupClonedIndexKey = new StepKey(phase, NAME, CleanupGeneratedIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyStep.NAME);
        StepKey generateCloneIndexNameKey = new StepKey(phase, NAME, GenerateUniqueIndexNameStep.NAME);
        StepKey cloneIndexKey = new StepKey(phase, NAME, ResizeIndexStep.CLONE);
        StepKey waitForClonedIndexGreenKey = new StepKey(phase, NAME, WAIT_FOR_CLONED_INDEX_GREEN);

        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey segmentCountKey = new StepKey(phase, NAME, SegmentCountStep.NAME);

        StepKey conditionalConfigureClonedIndexKey = new StepKey(phase, NAME, CONDITIONAL_CONFIGURE_CLONED_INDEX_STEP);
        StepKey reconfigureReplicasKey = new StepKey(phase, NAME, UPDATE_CLONED_INDEX_SETTINGS_STEP);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey aliasDataStreamBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATA_STREAM_CHECK_STEP);
        StepKey aliasSwapAndDeleteKey = new StepKey(phase, NAME, ShrinkSetAliasStep.NAME);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteSourceIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);

        // If the index is mounted as a searchable snapshot, skip the whole force-merge action
        BranchingStep conditionalSkipForceMergeStep = new BranchingStep(
            conditionalSkipForceMergeKey,
            checkNotWriteIndexKey,
            nextStepKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    String policyName = indexMetadata.getLifecyclePolicyName();
                    logger.warn(
                        "[{}] action is configured for index [{}] in policy [{}] which is mounted as searchable snapshot. "
                            + "Skipping this action",
                        ForceMergeAction.NAME,
                        index.getName(),
                        policyName
                    );
                    return true;
                }
                return false;
            }
        );

        // Wait for the index to not be the write index of a data stream
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndexKey,
            waitUntilTimeSeriesEndTimeKey
        );

        // If the index is a time series index, wait until its end time has passed
        WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
            waitUntilTimeSeriesEndTimeKey,
            conditionalSkipCloneKey,
            Instant::now
        );

        // If the index already has 0 replicas, we can skip the clone steps. If the action is configured to change the codec, we must
        // first update the codec of the original index before proceeding to the force-merge step.
        BranchingStep conditionalSkipCloneStep = new BranchingStep(
            conditionalSkipCloneKey,
            cleanupClonedIndexKey,
            codecChange ? closeIndexKey : forceMergeKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                return indexMetadata.getNumberOfReplicas() == 0;
            }
        );

        // Closing the index is required to change the index codec.
        CloseIndexStep closeIndexStep = new CloseIndexStep(closeIndexKey, updateBestCompressionKey, client);
        UpdateSettingsStep updateBestCompressionStep = new UpdateSettingsStep(
            updateBestCompressionKey,
            openIndexKey,
            client,
            BEST_COMPRESSION_SETTINGS
        );
        OpenIndexStep openIndexStep = new OpenIndexStep(openIndexKey, waitForGreenIndexKey, client);
        WaitForIndexColorStep waitForGreenIndexStep = new WaitForIndexColorStep(
            waitForGreenIndexKey,
            forceMergeKey,
            ClusterHealthStatus.GREEN
        );

        // If a previous force-merge action created a clone index but the action did not complete, we need to clean up the old clone index.
        CleanupGeneratedIndexStep cleanupClonedIndexStep = new CleanupGeneratedIndexStep(
            cleanupClonedIndexKey,
            readOnlyKey,
            client,
            FORCE_MERGE_INDEX_NAME_SUPPLIER
        );
        // The readOnlyKey used to exist for BwC reasons (as the step was removed at some point). It has now been reintroduced with the
        // changes to make the force-merge action use a cloned index. Therefore, we intentionally put this step before the
        // GenerateUniqueIndexNameStep so that an old index that was in the read-only step during an upgrade will still generate the name
        // and proceed with the rest of the action.
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, generateCloneIndexNameKey, client, false);
        GenerateUniqueIndexNameStep generateCloneIndexNameStep = new GenerateUniqueIndexNameStep(
            generateCloneIndexNameKey,
            cloneIndexKey,
            FORCE_MERGE_INDEX_PREFIX,
            (generatedIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setForceMergeIndexName(generatedIndexName)
        );
        // Clone the index with 0 replicas and the best compression codec if configured.
        ResizeIndexStep cloneIndexStep = new ResizeIndexStep(
            cloneIndexKey,
            waitForClonedIndexGreenKey,
            client,
            ResizeType.CLONE,
            FORCE_MERGE_INDEX_NAME_SUPPLIER,
            codecChange ? CLONE_SETTINGS_WITH_CODEC_SUPPLIER : CLONE_SETTINGS_WITHOUT_CODEC_SUPPLIER,
            null
        );
        // Wait for the cloned index to be green before proceeding with the force-merge. We wrap this with a
        // ClusterStateWaitUntilThresholdStep to avoid waiting forever if the index cannot be started for some reason. On timeout,
        // ILM will move back to the cleanup step, remove the cloned index, and retry the clone.
        ClusterStateWaitUntilThresholdStep waitForClonedIndexGreenStep = new ClusterStateWaitUntilThresholdStep(
            new WaitForIndexColorStep(
                waitForClonedIndexGreenKey,
                forceMergeKey,
                ClusterHealthStatus.GREEN,
                FORCE_MERGE_INDEX_NAME_SUPPLIER
            ),
            cleanupClonedIndexKey
        );

        // Execute the force merge (either on the original index or the cloned index).
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, segmentCountKey, client, maxNumSegments);
        // This step simply logs whether the force-merge resulted in the desired number of segments or not.
        SegmentCountStep segmentCountStep = new SegmentCountStep(
            segmentCountKey,
            conditionalConfigureClonedIndexKey,
            client,
            maxNumSegments
        );

        // If we cloned the index, we need to complete the setup of the cloned index and swap it with the original index.
        // If we did not clone the index, there's nothing else for us to do.
        BranchingStep conditionalConfigureClonedIndexStep = new BranchingStep(
            conditionalConfigureClonedIndexKey,
            nextStepKey,
            reconfigureReplicasKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                String cloneIndexName = indexMetadata.getLifecycleExecutionState().forceMergeIndexName();
                if (cloneIndexName == null) {
                    return false;
                }
                // If for some reason the cloned index does not exist in the cluster state, we don't want to fail the next steps,
                // so we skip them. This should not happen in ordinary circumstances.
                boolean clonedIndexExists = project.index(cloneIndexName) != null;
                assert clonedIndexExists
                    : "index [" + index.getName() + "] has cloned index name [" + cloneIndexName + "] but it does not exist in the cluster";
                return clonedIndexExists;
            }
        );

        // Reset the number of replicas to the value of the original index and remove the write block.
        UpdateSettingsStep reconfigureReplicasStep = new UpdateSettingsStep(
            reconfigureReplicasKey,
            copyMetadataKey,
            client,
            FORCE_MERGE_INDEX_NAME_SUPPLIER,
            (indexMetadata) -> Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, indexMetadata.getNumberOfReplicas())
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), (String) null)
                .build()
        );
        // Copy the lifecycle execution state from the original index to the cloned index. This also remove the skip setting so that
        // the cloned index can continue through the ILM phases.
        CopyExecutionStateStep copyMetadata = new CopyExecutionStateStep(
            copyMetadataKey,
            aliasDataStreamBranchingKey,
            FORCE_MERGE_INDEX_NAME_SUPPLIER,
            nextStepKey
        );
        // By the time we get to this step we have 2 indices, the source and the cloned one. We now need to choose an index
        // swapping strategy such that the cloned index takes the place of the source index (which should also be deleted).
        // If the source index is part of a data stream it's a matter of replacing it with the cloned index one in the data stream and
        // then deleting the source index; otherwise we'll use the alias management API to atomically transfer the aliases from
        // the source index to the cloned index and delete the source.
        BranchingStep aliasDataStreamBranchingStep = new BranchingStep(
            aliasDataStreamBranchingKey,
            aliasSwapAndDeleteKey,
            replaceDataStreamIndexKey,
            (index, project) -> {
                IndexAbstraction indexAbstraction = project.getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            }
        );
        SwapAliasesAndDeleteSourceIndexStep aliasSwapAndDeleteStep = new SwapAliasesAndDeleteSourceIndexStep(
            aliasSwapAndDeleteKey,
            nextStepKey,
            client,
            FORCE_MERGE_INDEX_NAME_SUPPLIER,
            true
        );
        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndexStep = new ReplaceDataStreamBackingIndexStep(
            replaceDataStreamIndexKey,
            deleteSourceIndexKey,
            FORCE_MERGE_INDEX_NAME_SUPPLIER
        );
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteSourceIndexKey, nextStepKey, client);

        return List.of(
            conditionalSkipForceMergeStep,
            checkNotWriteIndexStep,
            waitUntilTimeSeriesEndTimeStep,
            conditionalSkipCloneStep,
            closeIndexStep,
            updateBestCompressionStep,
            openIndexStep,
            waitForGreenIndexStep,
            cleanupClonedIndexStep,
            readOnlyStep,
            generateCloneIndexNameStep,
            cloneIndexStep,
            waitForClonedIndexGreenStep,
            forceMergeStep,
            segmentCountStep,
            conditionalConfigureClonedIndexStep,
            reconfigureReplicasStep,
            copyMetadata,
            aliasDataStreamBranchingStep,
            aliasSwapAndDeleteStep,
            replaceDataStreamBackingIndexStep,
            deleteSourceIndexStep
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, codec);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ForceMergeAction other = (ForceMergeAction) obj;
        return Objects.equals(this.maxNumSegments, other.maxNumSegments) && Objects.equals(this.codec, other.codec);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
