package table

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestCommitSnapshotFromManifests(t *testing.T) {
	ctx := context.Background()
	memoryIO := newInMemoryWriteFileIO()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}},
	)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "s3://bucket/test/location", iceberg.Properties{})
	require.NoError(t, err)

	tbl := New([]string{"default", "tbl"}, meta, "s3://bucket/test/location/metadata/v1.json",
		func(context.Context) (iceio.IO, error) {
			return memoryIO, nil
		}, nil)
	txn := tbl.NewTransaction()
	var expectedSequence int64
	if txn.meta.lastSequenceNumber == nil {
		expectedSequence = 0
	} else {
		expectedSequence = *txn.meta.lastSequenceNumber + 1
	}
	snapshotID := txn.meta.ReserveSnapshotID()
	manifest := iceberg.NewManifestFile(
		meta.Version(),
		"s3://bucket/test/location/metadata/manifest-1.avro",
		128,
		int32(spec.ID()),
		snapshotID,
	).
		AddedFiles(1).
		AddedRows(5).
		Build()
	summary := Summary{Operation: OpAppend, Properties: iceberg.Properties{"added-data-files": "1"}}

	require.NoError(t, txn.commitSnapshotFromManifests(ctx, snapshotID, nil, []iceberg.ManifestFile{manifest}, summary))

	require.Len(t, txn.meta.snapshotList, 1)
	snap := txn.meta.snapshotList[0]
	require.Equal(t, snapshotID, snap.SnapshotID)
	require.Nil(t, snap.ParentSnapshotID)
	require.Equal(t, expectedSequence, snap.SequenceNumber)
	require.NotEmpty(t, snap.ManifestList)
	require.NotNil(t, snap.Summary)
	require.Equal(t, summary.Operation, snap.Summary.Operation)
	require.Equal(t, summary.Properties, snap.Summary.Properties)
	require.NotNil(t, txn.meta.currentSnapshotID)
	require.Equal(t, snapshotID, *txn.meta.currentSnapshotID)
	require.Equal(t, snapshotID, txn.meta.refs[MainBranch].SnapshotID)
	require.Len(t, txn.meta.snapshotLog, 1)
	require.Equal(t, snapshotID, txn.meta.snapshotLog[0].SnapshotID)
	require.Len(t, txn.reqs, 1)
	require.IsType(t, &assertRefSnapshotID{}, txn.reqs[0])
	assertReq := txn.reqs[0].(*assertRefSnapshotID)
	require.Equal(t, MainBranch, assertReq.Ref)
	require.NotNil(t, txn.meta.lastSequenceNumber)
	require.Equal(t, snap.SequenceNumber, *txn.meta.lastSequenceNumber)

	data, ok := memoryIO.readFile(snap.ManifestList)
	require.True(t, ok)
	entries, err := iceberg.ReadManifestList(bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, manifest.FilePath(), entries[0].FilePath())
}

type inMemoryWriteFileIO struct {
	mu    sync.Mutex
	files map[string]*bytes.Buffer
}

func newInMemoryWriteFileIO() *inMemoryWriteFileIO {
	return &inMemoryWriteFileIO{files: make(map[string]*bytes.Buffer)}
}

func (m *inMemoryWriteFileIO) Open(name string) (iceio.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf, ok := m.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return newBufferFile(name, buf.Bytes()), nil
}

func (m *inMemoryWriteFileIO) Create(name string) (iceio.FileWriter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf := &bytes.Buffer{}
	m.files[name] = buf
	return &bufferFileWriter{Buffer: buf}, nil
}

func (m *inMemoryWriteFileIO) WriteFile(name string, content []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = bytes.NewBuffer(content)
	return nil
}

func (m *inMemoryWriteFileIO) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	return nil
}

func (m *inMemoryWriteFileIO) readFile(name string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf, ok := m.files[name]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), buf.Bytes()...), true
}

type bufferFileWriter struct {
	*bytes.Buffer
}

func (b *bufferFileWriter) Close() error { return nil }

func (b *bufferFileWriter) ReadFrom(r io.Reader) (int64, error) {
	return b.Buffer.ReadFrom(r)
}

type bufferFile struct {
	name string
	size int64
	data *bytes.Reader
}

func newBufferFile(name string, content []byte) *bufferFile {
	return &bufferFile{name: name, size: int64(len(content)), data: bytes.NewReader(append([]byte(nil), content...))}
}

func (b *bufferFile) Read(p []byte) (int, error) { return b.data.Read(p) }

func (b *bufferFile) Close() error { return nil }

func (b *bufferFile) Stat() (fs.FileInfo, error) { return memFileInfo{name: b.name, size: b.size}, nil }

func (b *bufferFile) Seek(offset int64, whence int) (int64, error) {
	return b.data.Seek(offset, whence)
}

func (b *bufferFile) ReadAt(p []byte, off int64) (int, error) { return b.data.ReadAt(p, off) }

type memFileInfo struct {
	name string
	size int64
}

func (m memFileInfo) Name() string       { return m.name }
func (m memFileInfo) Size() int64        { return m.size }
func (m memFileInfo) Mode() fs.FileMode  { return 0 }
func (m memFileInfo) ModTime() time.Time { return time.Time{} }
func (m memFileInfo) IsDir() bool        { return false }
func (m memFileInfo) Sys() any           { return nil }
