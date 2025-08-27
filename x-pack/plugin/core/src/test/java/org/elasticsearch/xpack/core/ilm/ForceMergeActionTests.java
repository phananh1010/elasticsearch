/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ForceMergeActionTests extends AbstractActionTestCase<ForceMergeAction> {

    @Override
    protected ForceMergeAction doParseInstance(XContentParser parser) {
        return ForceMergeAction.parse(parser);
    }

    @Override
    protected ForceMergeAction createTestInstance() {
        return randomInstance();
    }

    static ForceMergeAction randomInstance() {
        return new ForceMergeAction(randomIntBetween(1, 100), createRandomCompressionSettings());
    }

    static String createRandomCompressionSettings() {
        if (randomBoolean()) {
            return null;
        }
        return CodecService.BEST_COMPRESSION_CODEC;
    }

    @Override
    protected ForceMergeAction mutateInstance(ForceMergeAction instance) {
        int maxNumSegments = instance.getMaxNumSegments();
        maxNumSegments = maxNumSegments + randomIntBetween(1, 10);
        return new ForceMergeAction(maxNumSegments, createRandomCompressionSettings());
    }

    @Override
    protected Reader<ForceMergeAction> instanceReader() {
        return ForceMergeAction::new;
    }

    @Override
    public void testToSteps() {
        ForceMergeAction instance = randomInstance();
        boolean hasCodec = instance.getCodec() != null;
        String phase = randomAlphaOfLength(5);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        List<Step> steps = instance.toSteps(null, phase, nextStepKey);

        assertThat(steps.size(), equalTo(22));

        StepKey conditionalSkipForceMergeKey = new StepKey(
            phase,
            ForceMergeAction.NAME,
            ForceMergeAction.CONDITIONAL_SKIP_FORCE_MERGE_STEP
        );
        StepKey checkNotWriteIndexKey = new StepKey(phase, ForceMergeAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitUntilTimeSeriesEndTimeKey = new StepKey(phase, ForceMergeAction.NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey conditionalSkipCloneKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeAction.CONDITIONAL_SKIP_CLONE_STEP);
        StepKey closeIndexKey = new StepKey(phase, ForceMergeAction.NAME, CloseIndexStep.NAME);
        StepKey updateBestCompressionKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeAction.UPDATE_COMPRESSION_SETTINGS_STEP);
        StepKey openIndexKey = new StepKey(phase, ForceMergeAction.NAME, OpenIndexStep.NAME);
        StepKey waitForGreenIndexKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeAction.WAIT_FOR_COMPRESSION_SETTINGS_GREEN);
        StepKey cleanupClonedIndexKey = new StepKey(phase, ForceMergeAction.NAME, CleanupGeneratedIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, ForceMergeAction.NAME, ReadOnlyStep.NAME);
        StepKey generateCloneIndexNameKey = new StepKey(phase, ForceMergeAction.NAME, GenerateUniqueIndexNameStep.NAME);
        StepKey cloneIndexKey = new StepKey(phase, ForceMergeAction.NAME, ResizeIndexStep.CLONE);
        StepKey waitForClonedIndexGreenKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeAction.WAIT_FOR_CLONED_INDEX_GREEN);
        StepKey forceMergeKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeStep.NAME);
        StepKey segmentCountKey = new StepKey(phase, ForceMergeAction.NAME, SegmentCountStep.NAME);
        StepKey conditionalConfigureClonedIndexKey = new StepKey(
            phase,
            ForceMergeAction.NAME,
            ForceMergeAction.CONDITIONAL_CONFIGURE_CLONED_INDEX_STEP
        );
        StepKey reconfigureReplicasKey = new StepKey(phase, ForceMergeAction.NAME, ForceMergeAction.UPDATE_CLONED_INDEX_SETTINGS_STEP);
        StepKey copyMetadataKey = new StepKey(phase, ForceMergeAction.NAME, CopyExecutionStateStep.NAME);
        StepKey aliasDataStreamBranchingKey = new StepKey(
            phase,
            ForceMergeAction.NAME,
            ForceMergeAction.CONDITIONAL_DATA_STREAM_CHECK_STEP
        );
        StepKey aliasSwapAndDeleteKey = new StepKey(phase, ForceMergeAction.NAME, ShrinkSetAliasStep.NAME);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, ForceMergeAction.NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteSourceIndexKey = new StepKey(phase, ForceMergeAction.NAME, DeleteStep.NAME);

        assertTrue(steps.get(0) instanceof BranchingStep);
        assertThat(steps.get(0).getKey(), equalTo(conditionalSkipForceMergeKey));
        assertThrows(IllegalStateException.class, () -> steps.get(0).getNextStepKey());
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnFalse(), equalTo(checkNotWriteIndexKey));
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnTrue(), equalTo(nextStepKey));

        assertTrue(steps.get(1) instanceof CheckNotDataStreamWriteIndexStep);
        assertThat(steps.get(1).getKey(), equalTo(checkNotWriteIndexKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(waitUntilTimeSeriesEndTimeKey));

        assertTrue(steps.get(2) instanceof WaitUntilTimeSeriesEndTimePassesStep);
        assertThat(steps.get(2).getKey(), equalTo(waitUntilTimeSeriesEndTimeKey));
        assertThat(steps.get(2).getNextStepKey(), equalTo(conditionalSkipCloneKey));

        assertTrue(steps.get(3) instanceof BranchingStep);
        assertThat(steps.get(3).getKey(), equalTo(conditionalSkipCloneKey));
        assertThrows(IllegalStateException.class, () -> steps.get(3).getNextStepKey());
        assertThat(((BranchingStep) steps.get(3)).getNextStepKeyOnFalse(), equalTo(cleanupClonedIndexKey));
        assertThat(((BranchingStep) steps.get(3)).getNextStepKeyOnTrue(), equalTo(hasCodec ? closeIndexKey : forceMergeKey));

        assertTrue(steps.get(4) instanceof CloseIndexStep);
        assertThat(steps.get(4).getKey(), equalTo(closeIndexKey));
        assertThat(steps.get(4).getNextStepKey(), equalTo(updateBestCompressionKey));

        assertTrue(steps.get(5) instanceof UpdateSettingsStep);
        assertThat(steps.get(5).getKey(), equalTo(updateBestCompressionKey));
        assertThat(steps.get(5).getNextStepKey(), equalTo(openIndexKey));

        assertTrue(steps.get(6) instanceof OpenIndexStep);
        assertThat(steps.get(6).getKey(), equalTo(openIndexKey));
        assertThat(steps.get(6).getNextStepKey(), equalTo(waitForGreenIndexKey));

        assertTrue(steps.get(7) instanceof WaitForIndexColorStep);
        assertThat(steps.get(7).getKey(), equalTo(waitForGreenIndexKey));
        assertThat(steps.get(7).getNextStepKey(), equalTo(forceMergeKey));

        assertTrue(steps.get(8) instanceof CleanupGeneratedIndexStep);
        assertThat(steps.get(8).getKey(), equalTo(cleanupClonedIndexKey));
        assertThat(steps.get(8).getNextStepKey(), equalTo(readOnlyKey));

        assertTrue(steps.get(9) instanceof ReadOnlyStep);
        assertThat(steps.get(9).getKey(), equalTo(readOnlyKey));
        assertThat(steps.get(9).getNextStepKey(), equalTo(generateCloneIndexNameKey));

        assertTrue(steps.get(10) instanceof GenerateUniqueIndexNameStep);
        assertThat(steps.get(10).getKey(), equalTo(generateCloneIndexNameKey));
        assertThat(steps.get(10).getNextStepKey(), equalTo(cloneIndexKey));

        assertTrue(steps.get(11) instanceof ResizeIndexStep);
        assertThat(steps.get(11).getKey(), equalTo(cloneIndexKey));
        assertThat(steps.get(11).getNextStepKey(), equalTo(waitForClonedIndexGreenKey));

        assertTrue(steps.get(12) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(steps.get(12).getKey(), equalTo(waitForClonedIndexGreenKey));
        assertThat(steps.get(12).getNextStepKey(), equalTo(forceMergeKey));

        assertTrue(steps.get(13) instanceof ForceMergeStep);
        assertThat(steps.get(13).getKey(), equalTo(forceMergeKey));
        assertThat(steps.get(13).getNextStepKey(), equalTo(segmentCountKey));

        assertTrue(steps.get(14) instanceof SegmentCountStep);
        assertThat(steps.get(14).getKey(), equalTo(segmentCountKey));
        assertThat(steps.get(14).getNextStepKey(), equalTo(conditionalConfigureClonedIndexKey));

        assertTrue(steps.get(15) instanceof BranchingStep);
        assertThat(steps.get(15).getKey(), equalTo(conditionalConfigureClonedIndexKey));
        assertThrows(IllegalStateException.class, () -> steps.get(15).getNextStepKey());
        assertThat(((BranchingStep) steps.get(15)).getNextStepKeyOnFalse(), equalTo(nextStepKey));
        assertThat(((BranchingStep) steps.get(15)).getNextStepKeyOnTrue(), equalTo(reconfigureReplicasKey));

        assertTrue(steps.get(16) instanceof UpdateSettingsStep);
        assertThat(steps.get(16).getKey(), equalTo(reconfigureReplicasKey));
        assertThat(steps.get(16).getNextStepKey(), equalTo(copyMetadataKey));

        assertTrue(steps.get(17) instanceof CopyExecutionStateStep);
        assertThat(steps.get(17).getKey(), equalTo(copyMetadataKey));
        assertThat(steps.get(17).getNextStepKey(), equalTo(aliasDataStreamBranchingKey));

        assertTrue(steps.get(18) instanceof BranchingStep);
        assertThat(steps.get(18).getKey(), equalTo(aliasDataStreamBranchingKey));
        assertThrows(IllegalStateException.class, () -> steps.get(18).getNextStepKey());
        assertThat(((BranchingStep) steps.get(18)).getNextStepKeyOnFalse(), equalTo(aliasSwapAndDeleteKey));
        assertThat(((BranchingStep) steps.get(18)).getNextStepKeyOnTrue(), equalTo(replaceDataStreamIndexKey));

        assertTrue(steps.get(19) instanceof SwapAliasesAndDeleteSourceIndexStep);
        assertThat(steps.get(19).getKey(), equalTo(aliasSwapAndDeleteKey));
        assertThat(steps.get(19).getNextStepKey(), equalTo(nextStepKey));

        assertTrue(steps.get(20) instanceof ReplaceDataStreamBackingIndexStep);
        assertThat(steps.get(20).getKey(), equalTo(replaceDataStreamIndexKey));
        assertThat(steps.get(20).getNextStepKey(), equalTo(deleteSourceIndexKey));

        assertTrue(steps.get(21) instanceof DeleteStep);
        assertThat(steps.get(21).getKey(), equalTo(deleteSourceIndexKey));
        assertThat(steps.get(21).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testMissingMaxNumSegments() throws IOException {
        BytesReference emptyObject = BytesReference.bytes(JsonXContent.contentBuilder().startObject().endObject());
        XContentParser parser = XContentHelper.createParser(
            null,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            emptyObject,
            XContentType.JSON
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> ForceMergeAction.parse(parser));
        assertThat(e.getMessage(), equalTo("Required [max_num_segments]"));
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(-10, 0), null));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }

    public void testInvalidCodec() {
        Exception r = expectThrows(
            IllegalArgumentException.class,
            () -> new ForceMergeAction(randomIntBetween(1, 10), "DummyCompressingStoredFields")
        );
        assertThat(r.getMessage(), equalTo("unknown index codec: [DummyCompressingStoredFields]"));
    }
}
