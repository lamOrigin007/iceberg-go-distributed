// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"fmt"
	"maps"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// DistributedSnapshot captures the metadata required to coordinate a distributed
// snapshot commit between a coordinator and multiple worker hosts.
type DistributedSnapshot struct {
	// SnapshotID is the reserved identifier that every worker host must embed in
	// the manifest files it produces for this distributed commit.
	SnapshotID int64
	// ParentSnapshotID references the snapshot that was current when the
	// reservation happened. Coordinators pass the value back to
	// CommitDistributedSnapshot so optimistic concurrency checks can verify that
	// the branch head has not advanced since.
	ParentSnapshotID *int64
	// CommitUUID is a randomly generated value that can be used to derive
	// deterministic manifest file names across workers.
	CommitUUID uuid.UUID
	// SnapshotProps contains snapshot-level properties that should be attached to
	// the final snapshot summary. Callers should treat the map as immutable.
	SnapshotProps iceberg.Properties
}

// BeginDistributedSnapshot reserves a snapshot ID and returns the metadata that
// external components need to coordinate manifest writes. The returned
// DistributedSnapshot is immutable; callers should treat SnapshotProps as
// read-only.
func (t *Table) BeginDistributedSnapshot(_ context.Context, props iceberg.Properties) (*DistributedSnapshot, error) {
	meta, err := MetadataBuilderFromBase(t.metadata, t.metadataLocation)
	if err != nil {
		return nil, err
	}

	snapshotID := meta.ReserveSnapshotID()
	var parentSnapshotID *int64
	if current := t.metadata.CurrentSnapshot(); current != nil {
		id := current.SnapshotID
		parentSnapshotID = &id
	}

	var snapshotProps iceberg.Properties
	if len(props) > 0 {
		snapshotProps = maps.Clone(props)
	}

	return &DistributedSnapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
		CommitUUID:       uuid.New(),
		SnapshotProps:    snapshotProps,
	}, nil
}

// CommitDistributedSnapshot finalizes a distributed snapshot by assembling the
// supplied manifest files into a manifest list and committing the resulting
// snapshot metadata. The coordinator must pass the DistributedSnapshot produced
// by BeginDistributedSnapshot, the manifests created by worker hosts, and a
// summary map containing the required "operation" key.
func (t *Table) CommitDistributedSnapshot(
	ctx context.Context,
	ds *DistributedSnapshot,
	manifests []iceberg.ManifestFile,
	summary map[string]string,
) (*Table, error) {
	if ds == nil {
		return nil, fmt.Errorf("%w: distributed snapshot descriptor cannot be nil", iceberg.ErrInvalidArgument)
	}
	if len(manifests) == 0 {
		return nil, fmt.Errorf("%w: at least one manifest file is required", iceberg.ErrInvalidArgument)
	}

	summaryRecord, err := distributedSnapshotSummary(summary, ds.SnapshotProps)
	if err != nil {
		return nil, err
	}

	txn := t.NewTransaction()
	if err := txn.commitSnapshotFromManifestsWithRequirement(
		ctx,
		ds.SnapshotID,
		ds.ParentSnapshotID,
		manifests,
		summaryRecord,
		ds.ParentSnapshotID,
	); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

func distributedSnapshotSummary(summary map[string]string, props iceberg.Properties) (Summary, error) {
	if len(summary) == 0 {
		return Summary{}, ErrMissingOperation
	}
	operation, ok := summary[operationKey]
	if !ok {
		return Summary{}, ErrMissingOperation
	}

	op, err := ValidOperation(operation)
	if err != nil {
		return Summary{}, err
	}

	var finalProps iceberg.Properties
	if len(summary) > 1 {
		finalProps = make(iceberg.Properties, len(summary)-1)
		for k, v := range summary {
			if k == operationKey {
				continue
			}
			finalProps[k] = v
		}
	}

	if len(props) > 0 {
		if finalProps == nil {
			finalProps = make(iceberg.Properties, len(props))
		}
		maps.Copy(finalProps, props)
	}

	return Summary{Operation: op, Properties: finalProps}, nil
}
