/*
 * DDTxnProcessor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FOUNDATIONDB_DDTXNPROCESSOR_H
#define FOUNDATIONDB_DDTXNPROCESSOR_H

#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"

struct InitialDataDistribution;

/* Testability Contract:
 * a. The DataDistributor has to use this interface to interact with data-plane (aka. run transaction), because the
 * testability benefits from a mock implementation; b. Other control-plane roles should consider providing its own
 * TxnProcessor interface to provide testability, for example, Ratekeeper.
 * */
class IDDTxnProcessor {
public:
	struct SourceServers {
		std::vector<UID> srcServers, completeSources; // the same as RelocateData.src, RelocateData.completeSources;
	};
	// get the source server list and complete source server list for range
	virtual Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) = 0;

	// get the storage server list and Process class
	virtual Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses() = 0;

	virtual Future<Reference<InitialDataDistribution>> getInitialDataDistribution(
	    const UID& distributorId,
	    const MoveKeysLock& moveKeysLock,
	    const std::vector<Optional<Key>>& remoteDcIds,
	    const DDEnabledState* ddEnabledState) = 0;

	virtual ~IDDTxnProcessor() = default;

	[[nodiscard]] virtual Future<MoveKeysLock> takeMoveKeysLock(UID ddId) const { return MoveKeysLock(); }

	virtual Future<DatabaseConfiguration> getDatabaseConfiguration() const { return DatabaseConfiguration(); }

	virtual Future<Void> updateReplicaKeys(const std::vector<Optional<Key>>& primaryIds,
	                                       const std::vector<Optional<Key>>& remoteIds,
	                                       const DatabaseConfiguration& configuration) const {
		return Void();
	}

	virtual Future<Void> waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const = 0;
};

class DDTxnProcessorImpl;

// run transactions over real database
class DDTxnProcessor : public IDDTxnProcessor {
	friend class DDTxnProcessorImpl;
	Database cx;

public:
	DDTxnProcessor() = default;
	explicit DDTxnProcessor(Database cx) : cx(cx) {}

	Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) override;

	// Call NativeAPI implementation directly
	Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses() override;

	Future<Reference<InitialDataDistribution>> getInitialDataDistribution(
	    const UID& distributorId,
	    const MoveKeysLock& moveKeysLock,
	    const std::vector<Optional<Key>>& remoteDcIds,
	    const DDEnabledState* ddEnabledState) override;

	Future<MoveKeysLock> takeMoveKeysLock(UID ddId) const override;

	Future<DatabaseConfiguration> getDatabaseConfiguration() const override;

	Future<Void> updateReplicaKeys(const std::vector<Optional<Key>>& primaryIds,
	                               const std::vector<Optional<Key>>& remoteIds,
	                               const DatabaseConfiguration& configuration) const override;

	Future<Void> waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const override;
};

// A mock transaction implementation for test usage.
// Contract: every function involving mock transaction should return immediately to mimic the ACI property of real
// transaction.
class DDMockTxnProcessor : public IDDTxnProcessor {};

#endif // FOUNDATIONDB_DDTXNPROCESSOR_H
