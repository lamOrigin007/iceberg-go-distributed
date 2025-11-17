package table

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type distributedSnapshotTestCatalog struct {
	metadata Metadata
}

func (c *distributedSnapshotTestCatalog) LoadTable(context.Context, Identifier) (*Table, error) {
	return nil, fmt.Errorf("%w: not implemented", ErrInvalidOperation)
}

func (c *distributedSnapshotTestCatalog) CommitTable(
	ctx context.Context,
	ident Identifier,
	reqs []Requirement,
	updates []Update,
) (Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.metadata); err != nil {
			return nil, "", err
		}
	}

	bldr, err := MetadataBuilderFromBase(c.metadata, "")
	if err != nil {
		return nil, "", err
	}

	for _, u := range updates {
		if err := u.Apply(bldr); err != nil {
			return nil, "", err
		}
	}

	meta, err := bldr.Build()
	if err != nil {
		return nil, "", err
	}

	c.metadata = meta

	return meta, fmt.Sprintf("%s/metadata/%s.metadata.json", meta.Location(), uuid.New()), nil
}

func TestTableDistributedSnapshotLifecycle(t *testing.T) {
	ctx := context.Background()
	memIO := newInMemoryWriteFileIO()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"s3://bucket/test/location", iceberg.Properties{})
	require.NoError(t, err)

	cat := &distributedSnapshotTestCatalog{metadata: meta}
	identifier := Identifier{"default", "tbl"}
	tbl := New(identifier, meta, "s3://bucket/test/location/metadata/v1.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)

	firstProps := iceberg.Properties{"distributed": "true"}
	ds, err := tbl.BeginDistributedSnapshot(ctx, firstProps)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NotZero(t, ds.SnapshotID)
	require.Nil(t, ds.ParentSnapshotID)
	require.Equal(t, firstProps, ds.SnapshotProps)
	require.NotEqual(t, uuid.Nil, ds.CommitUUID)

	manifestPath := fmt.Sprintf("%s/metadata/%s-m1.avro", tbl.Location(), ds.CommitUUID)
	spec := meta.PartitionSpec()
	manifests := []iceberg.ManifestFile{
		iceberg.NewManifestFile(meta.Version(), manifestPath, 128, int32((&spec).ID()), ds.SnapshotID).
			AddedFiles(1).
			AddedRows(5).
			Build(),
	}
	summary := map[string]string{
		operationKey:       string(OpAppend),
		"added-data-files": "1",
	}

	updatedTbl, err := tbl.CommitDistributedSnapshot(ctx, ds, manifests, summary)
	require.NoError(t, err)
	require.Equal(t, ds.SnapshotID, updatedTbl.CurrentSnapshot().SnapshotID)
	require.Equal(t, int64(1), updatedTbl.CurrentSnapshot().SequenceNumber)
	require.Equal(t, OpAppend, updatedTbl.CurrentSnapshot().Summary.Operation)
	require.Equal(t, "true", updatedTbl.CurrentSnapshot().Summary.Properties["distributed"])

	verifyManifestList(t, memIO, updatedTbl.CurrentSnapshot(), manifests)

	secondProps := iceberg.Properties{"batch": "2"}
	secondDS, err := updatedTbl.BeginDistributedSnapshot(ctx, secondProps)
	require.NoError(t, err)
	require.NotNil(t, secondDS.ParentSnapshotID)
	require.Equal(t, ds.SnapshotID, *secondDS.ParentSnapshotID)

	secondManifestPath := fmt.Sprintf("%s/metadata/%s-m2.avro", updatedTbl.Location(), secondDS.CommitUUID)
	secondSpec := meta.PartitionSpec()
	secondManifests := []iceberg.ManifestFile{
		iceberg.NewManifestFile(meta.Version(), secondManifestPath, 256, int32((&secondSpec).ID()), secondDS.SnapshotID).
			AddedFiles(2).
			AddedRows(10).
			Build(),
	}
	secondSummary := map[string]string{
		operationKey:       string(OpAppend),
		"added-data-files": "2",
	}

	finalTbl, err := updatedTbl.CommitDistributedSnapshot(ctx, secondDS, secondManifests, secondSummary)
	require.NoError(t, err)
	require.Equal(t, secondDS.SnapshotID, finalTbl.CurrentSnapshot().SnapshotID)
	require.Equal(t, int64(2), finalTbl.CurrentSnapshot().SequenceNumber)
	require.Equal(t, 2, len(finalTbl.Metadata().Snapshots()))
	require.NotNil(t, finalTbl.CurrentSnapshot().ParentSnapshotID)
	require.Equal(t, ds.SnapshotID, *finalTbl.CurrentSnapshot().ParentSnapshotID)
	verifyManifestList(t, memIO, finalTbl.CurrentSnapshot(), secondManifests)
}

func TestCommitDistributedSnapshotFailsOnConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	memIO := newInMemoryWriteFileIO()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"s3://bucket/test/location", iceberg.Properties{})
	require.NoError(t, err)
	spec := meta.PartitionSpec()
	specID := int32((&spec).ID())

	cat := &distributedSnapshotTestCatalog{metadata: meta}
	identifier := Identifier{"default", "tbl"}
	tbl := New(identifier, meta, "s3://bucket/test/location/metadata/v1.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)

	bootstrapProps := iceberg.Properties{"stage": "bootstrap"}
	bootstrapDS, err := tbl.BeginDistributedSnapshot(ctx, bootstrapProps)
	require.NoError(t, err)
	bootstrapManifests := []iceberg.ManifestFile{
		iceberg.NewManifestFile(meta.Version(),
			fmt.Sprintf("%s/metadata/%s-bootstrap.avro", tbl.Location(), bootstrapDS.CommitUUID),
			64,
			specID,
			bootstrapDS.SnapshotID,
		).
			AddedFiles(1).
			AddedRows(5).
			Build(),
	}
	bootstrapSummary := map[string]string{operationKey: string(OpAppend)}
	tbl, err = tbl.CommitDistributedSnapshot(ctx, bootstrapDS, bootstrapManifests, bootstrapSummary)
	require.NoError(t, err)

	staleTable := *tbl
	coordinator := &staleTable
	pendingDS, err := coordinator.BeginDistributedSnapshot(ctx, iceberg.Properties{"attempt": "stale"})
	require.NoError(t, err)
	pendingManifests := []iceberg.ManifestFile{
		iceberg.NewManifestFile(meta.Version(),
			fmt.Sprintf("%s/metadata/%s-stale.avro", coordinator.Location(), pendingDS.CommitUUID),
			128,
			specID,
			pendingDS.SnapshotID,
		).
			AddedFiles(2).
			AddedRows(10).
			Build(),
	}
	pendingSummary := map[string]string{operationKey: string(OpAppend)}

	concurrentDS, err := tbl.BeginDistributedSnapshot(ctx, iceberg.Properties{"attempt": "winner"})
	require.NoError(t, err)
	concurrentManifests := []iceberg.ManifestFile{
		iceberg.NewManifestFile(meta.Version(),
			fmt.Sprintf("%s/metadata/%s-concurrent.avro", tbl.Location(), concurrentDS.CommitUUID),
			256,
			specID,
			concurrentDS.SnapshotID,
		).
			AddedFiles(3).
			AddedRows(15).
			Build(),
	}
	concurrentSummary := map[string]string{operationKey: string(OpAppend)}
	_, err = tbl.CommitDistributedSnapshot(ctx, concurrentDS, concurrentManifests, concurrentSummary)
	require.NoError(t, err)

	_, err = coordinator.CommitDistributedSnapshot(ctx, pendingDS, pendingManifests, pendingSummary)
	require.Error(t, err)
	require.ErrorContains(t, err, "requirement failed")
	require.Equal(t, concurrentDS.SnapshotID, cat.metadata.CurrentSnapshot().SnapshotID)
}

func verifyManifestList(t *testing.T, fs *inMemoryWriteFileIO, snap *Snapshot, manifests []iceberg.ManifestFile) {
	data, ok := fs.readFile(snap.ManifestList)
	require.True(t, ok)
	entries, err := iceberg.ReadManifestList(bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, entries, len(manifests))
	for i, entry := range entries {
		require.Equal(t, manifests[i].FilePath(), entry.FilePath())
	}
}
