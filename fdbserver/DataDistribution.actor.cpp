/*
 * DataDistribution.actor.cpp
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

#include <set>
#include <unordered_map>

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/Replication.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/BooleanParam.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

enum class DDAuditContext : uint8_t {
	INVALID = 0,
	RESUME = 1,
	LAUNCH = 2,
	RETRY = 3,
};

struct DDAudit {
	DDAudit(AuditStorageState coreState)
	  : coreState(coreState), actors(true), foundError(false), auditStorageAnyChildFailed(false), retryCount(0),
	    cancelled(false), overallCompleteDoAuditCount(0), overallIssuedDoAuditCount(0), overallSkippedDoAuditCount(0),
	    remainingBudgetForAuditTasks(SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX), context(DDAuditContext::INVALID) {}

	AuditStorageState coreState;
	ActorCollection actors;
	Future<Void> auditActor;
	bool foundError;
	bool auditStorageAnyChildFailed;
	int retryCount;
	bool cancelled; // use to cancel any actor beyond auditActor
	int64_t overallCompleteDoAuditCount;
	int64_t overallIssuedDoAuditCount;
	int64_t overallSkippedDoAuditCount;
	AsyncVar<int> remainingBudgetForAuditTasks;
	DDAuditContext context;
	std::unordered_set<UID> serversFinishedSSShardAudit; // dedicated to ssshard

	inline void setAuditRunActor(Future<Void> actor) { auditActor = actor; }
	inline Future<Void> getAuditRunActor() { return auditActor; }

	inline void setDDAuditContext(DDAuditContext context_) { this->context = context_; }
	inline DDAuditContext getDDAuditContext() const { return context; }

	// auditActor and actors are guaranteed to deliver a cancel signal
	void cancel() {
		auditActor.cancel();
		actors.clear(true);
		cancelled = true;
	}

	bool isCancelled() const { return cancelled; }
};

struct DDRangeLocations {
	DDRangeLocations() = default;
	DDRangeLocations(KeyRangeRef range) : range(range) {}

	// A map of dcId : list of servers
	std::map<std::string, std::vector<StorageServerInterface>> servers;
	KeyRange range;
};

// Read keyservers, return unique set of teams
ACTOR Future<Reference<InitialDataDistribution>> getInitialDataDistribution(Database cx,
                                                                            UID distributorId,
                                                                            MoveKeysLock moveKeysLock,
                                                                            std::vector<Optional<Key>> remoteDcIds,
                                                                            const DDEnabledState* ddEnabledState) {
	state Reference<InitialDataDistribution> result = makeReference<InitialDataDistribution>();
	state Key beginKey = allKeys.begin;

	state bool succeeded;

	state Transaction tr(cx);

	state std::map<UID, Optional<Key>> server_dc;
	state std::map<std::vector<UID>, std::pair<std::vector<UID>, std::vector<UID>>> team_cache;
	state std::vector<std::pair<StorageServerInterface, ProcessClass>> tss_servers;

	// Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure
	// causing entries to be duplicated
	loop {
		server_dc.clear();
		succeeded = false;
		try {

			// Read healthyZone value which is later used to determine on/off of failure triggered DD
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<Value> val = wait(tr.get(healthyZoneKey));
			if (val.present()) {
				auto p = decodeHealthyZoneValue(val.get());
				if (p.second > tr.getReadVersion().get() || p.first == ignoreSSFailuresZoneString) {
					result->initHealthyZoneValue = Optional<Key>(p.first);
				} else {
					result->initHealthyZoneValue = Optional<Key>();
				}
			} else {
				result->initHealthyZoneValue = Optional<Key>();
			}

			result->mode = 1;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
			if (mode.present()) {
				BinaryReader rd(mode.get(), Unversioned());
				rd >> result->mode;
			}
			if (!result->mode || !ddEnabledState->isDDEnabled()) {
				// DD can be disabled persistently (result->mode = 0) or transiently (isDDEnabled() = 0)
				TraceEvent(SevDebug, "GetInitialDataDistribution_DisabledDD").log();
				return result;
			}

			state Future<std::vector<ProcessData>> workers = getWorkers(&tr);
			state Future<RangeResult> serverList = tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			wait(success(workers) && success(serverList));
			ASSERT(!serverList.get().more && serverList.get().size() < CLIENT_KNOBS->TOO_MANY);

			std::map<Optional<Standalone<StringRef>>, ProcessData> id_data;
			for (int i = 0; i < workers.get().size(); i++)
				id_data[workers.get()[i].locality.processId()] = workers.get()[i];

			succeeded = true;

			for (int i = 0; i < serverList.get().size(); i++) {
				auto ssi = decodeServerListValue(serverList.get()[i].value);
				if (!ssi.isTss()) {
					result->allServers.emplace_back(ssi, id_data[ssi.locality.processId()].processClass);
					server_dc[ssi.id()] = ssi.locality.dcId();
				} else {
					tss_servers.emplace_back(ssi, id_data[ssi.locality.processId()].processClass);
				}
			}

			break;
		} catch (Error& e) {
			wait(tr.onError(e));

			ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this loop
			TraceEvent("GetInitialTeamsRetry", distributorId).log();
		}
	}

	// If keyServers is too large to read in a single transaction, then we will have to break this process up into
	// multiple transactions. In that case, each iteration should begin where the previous left off
	while (beginKey < allKeys.end) {
		TEST(beginKey > allKeys.begin); // Multi-transactional getInitialDataDistribution
		loop {
			succeeded = false;
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				wait(checkMoveKeysLockReadOnly(&tr, moveKeysLock, ddEnabledState));
				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult keyServers = wait(krmGetRanges(&tr,
				                                           keyServersPrefix,
				                                           KeyRangeRef(beginKey, allKeys.end),
				                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
				                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
				succeeded = true;

				std::vector<UID> src, dest, last;

				// for each range
				for (int i = 0; i < keyServers.size() - 1; i++) {
					DDShardInfo info(keyServers[i].key);
					decodeKeyServersValue(UIDtoTagMap, keyServers[i].value, src, dest);
					if (remoteDcIds.size()) {
						auto srcIter = team_cache.find(src);
						if (srcIter == team_cache.end()) {
							for (auto& id : src) {
								auto& dc = server_dc[id];
								if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
									info.remoteSrc.push_back(id);
								} else {
									info.primarySrc.push_back(id);
								}
							}
							result->primaryTeams.insert(info.primarySrc);
							result->remoteTeams.insert(info.remoteSrc);
							team_cache[src] = std::make_pair(info.primarySrc, info.remoteSrc);
						} else {
							info.primarySrc = srcIter->second.first;
							info.remoteSrc = srcIter->second.second;
						}
						if (dest.size()) {
							info.hasDest = true;
							auto destIter = team_cache.find(dest);
							if (destIter == team_cache.end()) {
								for (auto& id : dest) {
									auto& dc = server_dc[id];
									if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
										info.remoteDest.push_back(id);
									} else {
										info.primaryDest.push_back(id);
									}
								}
								result->primaryTeams.insert(info.primaryDest);
								result->remoteTeams.insert(info.remoteDest);
								team_cache[dest] = std::make_pair(info.primaryDest, info.remoteDest);
							} else {
								info.primaryDest = destIter->second.first;
								info.remoteDest = destIter->second.second;
							}
						}
					} else {
						info.primarySrc = src;
						auto srcIter = team_cache.find(src);
						if (srcIter == team_cache.end()) {
							result->primaryTeams.insert(src);
							team_cache[src] = std::pair<std::vector<UID>, std::vector<UID>>();
						}
						if (dest.size()) {
							info.hasDest = true;
							info.primaryDest = dest;
							auto destIter = team_cache.find(dest);
							if (destIter == team_cache.end()) {
								result->primaryTeams.insert(dest);
								team_cache[dest] = std::pair<std::vector<UID>, std::vector<UID>>();
							}
						}
					}
					result->shards.push_back(info);
				}

				ASSERT_GT(keyServers.size(), 0);
				beginKey = keyServers.end()[-1].key;
				break;
			} catch (Error& e) {
				TraceEvent("GetInitialTeamsKeyServersRetry", distributorId).error(e);

				wait(tr.onError(e));
				ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this loop
			}
		}

		tr.reset();
	}

	// a dummy shard at the end with no keys or servers makes life easier for trackInitialShards()
	result->shards.push_back(DDShardInfo(allKeys.end));

	// add tss to server list AFTER teams are built
	for (auto& it : tss_servers) {
		result->allServers.push_back(it);
	}

	return result;
}

// add server to wiggling queue
void StorageWiggler::addServer(const UID& serverId, const StorageMetadataType& metadata) {
	// std::cout << "size: " << pq_handles.size() << " add " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	ASSERT(!pq_handles.count(serverId));
	pq_handles[serverId] = wiggle_pq.emplace(metadata, serverId);
	nonEmpty.set(true);
}

void StorageWiggler::removeServer(const UID& serverId) {
	// std::cout << "size: " << pq_handles.size() << " remove " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	if (contains(serverId)) { // server haven't been popped
		auto handle = pq_handles.at(serverId);
		pq_handles.erase(serverId);
		wiggle_pq.erase(handle);
	}
	nonEmpty.set(!wiggle_pq.empty());
}

void StorageWiggler::updateMetadata(const UID& serverId, const StorageMetadataType& metadata) {
	//	std::cout << "size: " << pq_handles.size() << " update " << serverId.toString()
	//	          << " DC: " << teamCollection->isPrimary() << std::endl;
	auto handle = pq_handles.at(serverId);
	if ((*handle).first.createdTime == metadata.createdTime) {
		return;
	}
	wiggle_pq.update(handle, std::make_pair(metadata, serverId));
}

Optional<UID> StorageWiggler::getNextServerId() {
	if (!wiggle_pq.empty()) {
		auto [metadata, id] = wiggle_pq.top();
		wiggle_pq.pop();
		pq_handles.erase(id);
		return Optional<UID>(id);
	}
	return Optional<UID>();
}

Future<Void> StorageWiggler::resetStats() {
	auto newMetrics = StorageWiggleMetrics();
	newMetrics.smoothed_round_duration = metrics.smoothed_round_duration;
	newMetrics.smoothed_wiggle_duration = metrics.smoothed_wiggle_duration;
	return StorageWiggleMetrics::runSetTransaction(teamCollection->cx, teamCollection->isPrimary(), newMetrics);
}

Future<Void> StorageWiggler::restoreStats() {
	auto& metricsRef = metrics;
	auto assignFunc = [&metricsRef](Optional<Value> v) {
		if (v.present()) {
			metricsRef = BinaryReader::fromStringRef<StorageWiggleMetrics>(v.get(), IncludeVersion());
		}
		return Void();
	};
	auto readFuture = StorageWiggleMetrics::runGetTransaction(teamCollection->cx, teamCollection->isPrimary());
	return map(readFuture, assignFunc);
}
Future<Void> StorageWiggler::startWiggle() {
	metrics.last_wiggle_start = StorageMetadataType::currentTime();
	if (shouldStartNewRound()) {
		metrics.last_round_start = metrics.last_wiggle_start;
	}
	return StorageWiggleMetrics::runSetTransaction(teamCollection->cx, teamCollection->isPrimary(), metrics);
}

Future<Void> StorageWiggler::finishWiggle() {
	metrics.last_wiggle_finish = StorageMetadataType::currentTime();
	metrics.finished_wiggle += 1;
	auto duration = metrics.last_wiggle_finish - metrics.last_wiggle_start;
	metrics.smoothed_wiggle_duration.setTotal((double)duration);

	if (shouldFinishRound()) {
		metrics.last_round_finish = metrics.last_wiggle_finish;
		metrics.finished_round += 1;
		duration = metrics.last_round_finish - metrics.last_round_start;
		metrics.smoothed_round_duration.setTotal((double)duration);
	}
	return StorageWiggleMetrics::runSetTransaction(teamCollection->cx, teamCollection->isPrimary(), metrics);
}

ACTOR Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses(
    Transaction* tr) {
	state Future<std::vector<ProcessData>> workers = getWorkers(tr);
	state Future<RangeResult> serverList = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
	wait(success(workers) && success(serverList));
	ASSERT(!serverList.get().more && serverList.get().size() < CLIENT_KNOBS->TOO_MANY);

	std::map<Optional<Standalone<StringRef>>, ProcessData> id_data;
	for (int i = 0; i < workers.get().size(); i++)
		id_data[workers.get()[i].locality.processId()] = workers.get()[i];

	std::vector<std::pair<StorageServerInterface, ProcessClass>> results;
	for (int i = 0; i < serverList.get().size(); i++) {
		auto ssi = decodeServerListValue(serverList.get()[i].value);
		results.emplace_back(ssi, id_data[ssi.locality.processId()].processClass);
	}

	return results;
}

ACTOR Future<Void> remoteRecovered(Reference<AsyncVar<ServerDBInfo> const> db) {
	TraceEvent("DDTrackerStarting").log();
	while (db->get().recoveryState < RecoveryState::ALL_LOGS_RECRUITED) {
		TraceEvent("DDTrackerStarting").detail("RecoveryState", (int)db->get().recoveryState);
		wait(db->onChange());
	}
	return Void();
}

ACTOR Future<Void> waitForDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
	state Transaction tr(cx);
	loop {
		wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskPriority::DataDistribution));

		try {
			Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
			if (!mode.present() && ddEnabledState->isDDEnabled()) {
				TraceEvent("WaitForDDEnabledSucceeded").log();
				return Void();
			}
			if (mode.present()) {
				BinaryReader rd(mode.get(), Unversioned());
				int m;
				rd >> m;
				TraceEvent(SevDebug, "WaitForDDEnabled")
				    .detail("Mode", m)
				    .detail("IsDDEnabled", ddEnabledState->isDDEnabled());
				if (m && ddEnabledState->isDDEnabled()) {
					TraceEvent("WaitForDDEnabledSucceeded").log();
					return Void();
				}
			}

			tr.reset();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> isDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
			if (!mode.present() && ddEnabledState->isDDEnabled())
				return true;
			if (mode.present()) {
				BinaryReader rd(mode.get(), Unversioned());
				int m;
				rd >> m;
				if (m && ddEnabledState->isDDEnabled()) {
					TraceEvent(SevDebug, "IsDDEnabledSucceeded")
					    .detail("Mode", m)
					    .detail("IsDDEnabled", ddEnabledState->isDDEnabled());
					return true;
				}
			}
			// SOMEDAY: Write a wrapper in MoveKeys.actor.h
			Optional<Value> readVal = wait(tr.get(moveKeysLockOwnerKey));
			UID currentOwner =
			    readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			if (ddEnabledState->isDDEnabled() && (currentOwner != dataDistributionModeLock)) {
				TraceEvent(SevDebug, "IsDDEnabledSucceeded")
				    .detail("CurrentOwner", currentOwner)
				    .detail("DDModeLock", dataDistributionModeLock)
				    .detail("IsDDEnabled", ddEnabledState->isDDEnabled());
				return true;
			}
			TraceEvent(SevDebug, "IsDDEnabledFailed")
			    .detail("CurrentOwner", currentOwner)
			    .detail("DDModeLock", dataDistributionModeLock)
			    .detail("IsDDEnabled", ddEnabledState->isDDEnabled());
			return false;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Ensures that the serverKeys key space is properly coalesced
// This method is only used for testing and is not implemented in a manner that is safe for large databases
ACTOR Future<Void> debugCheckCoalescing(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			state RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			state int i;
			for (i = 0; i < serverList.size(); i++) {
				state UID id = decodeServerListValue(serverList[i].value).id();
				RangeResult ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(id), allKeys));
				ASSERT(ranges.end()[-1].key == allKeys.end);

				for (int j = 0; j < ranges.size() - 2; j++)
					if (ranges[j].value == ranges[j + 1].value)
						TraceEvent(SevError, "UncoalescedValues", id)
						    .detail("Key1", ranges[j].key)
						    .detail("Key2", ranges[j + 1].key)
						    .detail("Value", ranges[j].value);
			}

			TraceEvent("DoneCheckingCoalescing").log();
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

static std::set<int> const& normalDDQueueErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_movekeys_conflict);
		s.insert(error_code_broken_promise);
	}
	return s;
}

ACTOR Future<Void> pollMoveKeysLock(Database cx, MoveKeysLock lock, const DDEnabledState* ddEnabledState) {
	loop {
		wait(delay(SERVER_KNOBS->MOVEKEYS_LOCK_POLLING_DELAY));
		state Transaction tr(cx);
		loop {
			try {
				wait(checkMoveKeysLockReadOnly(&tr, lock, ddEnabledState));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

struct DataDistributorData : NonCopyable, ReferenceCounted<DataDistributorData> {
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	UID ddId;
	PromiseStream<Future<Void>> addActor;
	DDTeamCollection* teamCollection;
	MoveKeysLock lock;
	Database cx;
	DDEnabledState ddEnabledState;
	Reference<EventCacheHolder> initialDDEventHolder;
	Reference<EventCacheHolder> movingDataEventHolder;
	Reference<EventCacheHolder> totalDataInFlightEventHolder;
	Reference<EventCacheHolder> totalDataInFlightRemoteEventHolder;
	bool initialized;

	std::unordered_map<AuditType, std::unordered_map<UID, std::shared_ptr<DDAudit>>> audits;
	FlowLock auditStorageReplicaLaunchingLock;
	Promise<Void> auditStorageInitialized;
	bool auditStorageInitStarted;

	DataDistributorData(Reference<AsyncVar<ServerDBInfo> const> const& db, UID id)
	  : dbInfo(db), ddId(id), teamCollection(nullptr),
	    initialDDEventHolder(makeReference<EventCacheHolder>("InitialDD")),
	    movingDataEventHolder(makeReference<EventCacheHolder>("MovingData")),
	    totalDataInFlightEventHolder(makeReference<EventCacheHolder>("TotalDataInFlight")),
	    totalDataInFlightRemoteEventHolder(makeReference<EventCacheHolder>("TotalDataInFlightRemote")),
	    initialized(false), auditStorageReplicaLaunchingLock(1), auditStorageInitStarted(false) {}
};

inline void addAuditToAuditMap(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit) {
	AuditType auditType = audit->coreState.getType();
	UID auditID = audit->coreState.id;
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "addAuditToAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	// ASSERT(!self->audits[auditType].contains(auditID));
	self->audits[auditType][auditID] = audit;
	return;
}

inline std::shared_ptr<DDAudit> getAuditFromAuditMap(Reference<DataDistributorData> self,
                                                     AuditType auditType,
                                                     UID auditID) {
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "getAuditFromAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	// ASSERT(self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
	return self->audits[auditType][auditID];
}

inline void removeAuditFromAuditMap(Reference<DataDistributorData> self, AuditType auditType, UID auditID) {
	// ASSERT(self->audits.contains(auditType) && self->audits[auditType].contains(auditID));
	std::shared_ptr<DDAudit> audit = self->audits[auditType][auditID];
	audit->cancel();
	self->audits[auditType].erase(auditID);
	TraceEvent(SevDebug, "AuditMapOps", self->ddId)
	    .detail("Ops", "removeAuditFromAuditMap")
	    .detail("AuditType", auditType)
	    .detail("AuditID", auditID);
	return;
}

inline bool auditExistInAuditMap(Reference<DataDistributorData> self, AuditType auditType, UID auditID) {
	// return self->audits.contains(auditType) && self->audits[auditType].contains(auditID);
	auto it = self->audits.find(auditType);
	if (it == self->audits.end()) {
		return false;
	}

	return it->second.find(auditID) != it->second.end();
}

inline bool existAuditInAuditMapForType(Reference<DataDistributorData> self, AuditType auditType) {
	auto it = self->audits.find(auditType);
	if (it == self->audits.end()) {
		return false;
	}

	return !it->second.empty();
}

inline std::unordered_map<UID, std::shared_ptr<DDAudit>> getAuditsForType(Reference<DataDistributorData> self,
                                                                          AuditType auditType) {
	return self->audits[auditType];
}

void runAuditStorage(
    Reference<DataDistributorData> self,
    AuditStorageState auditStates,
    int retryCount,
    DDAuditContext context,
    Optional<std::unordered_set<UID>> serversFinishedSSShardAudit = Optional<std::unordered_set<UID>>());
ACTOR Future<Void> auditStorageCore(Reference<DataDistributorData> self,
                                    UID auditID,
                                    AuditType auditType,
                                    int currentRetryCount);
ACTOR Future<UID> launchAudit(Reference<DataDistributorData> self,
                              KeyRange auditRange,
                              AuditType auditType,
                              KeyValueStoreType auditStorageEngineType);
ACTOR Future<Void> auditStorage(Reference<DataDistributorData> self, TriggerAuditRequest req);
void loadAndDispatchAudit(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> dispatchAuditStorageServerShard(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> scheduleAuditStorageShardOnServer(Reference<DataDistributorData> self,
                                                     std::shared_ptr<DDAudit> audit,
                                                     StorageServerInterface ssi);
ACTOR Future<Void> dispatchAuditStorage(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit);
ACTOR Future<Void> dispatchAuditLocationMetadata(Reference<DataDistributorData> self,
                                                 std::shared_ptr<DDAudit> audit,
                                                 KeyRange range);
ACTOR Future<Void> doAuditLocationMetadata(Reference<DataDistributorData> self,
                                           std::shared_ptr<DDAudit> audit,
                                           KeyRange auditRange);
ACTOR Future<Void> scheduleAuditOnRange(Reference<DataDistributorData> self,
                                        std::shared_ptr<DDAudit> audit,
                                        KeyRange range);
ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributorData> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req);
ACTOR Future<Void> skipAuditOnRange(Reference<DataDistributorData> self,
                                    std::shared_ptr<DDAudit> audit,
                                    KeyRange rangeToSkip);

ACTOR Future<Void> monitorBatchLimitedTime(Reference<AsyncVar<ServerDBInfo> const> db, double* lastLimited) {
	loop {
		wait(delay(SERVER_KNOBS->METRIC_UPDATE_RATE));

		state Reference<GrvProxyInfo> grvProxies(new GrvProxyInfo(db->get().client.grvProxies));

		choose {
			when(wait(db->onChange())) {}
			when(GetHealthMetricsReply reply =
			         wait(grvProxies->size() ? basicLoadBalance(grvProxies,
			                                                    &GrvProxyInterface::getHealthMetrics,
			                                                    GetHealthMetricsRequest(false))
			                                 : Never())) {
				if (reply.healthMetrics.batchLimited) {
					*lastLimited = now();
				}
			}
		}
	}
}

// Runs the data distribution algorithm for FDB, including the DD Queue, DD tracker, and DD team collection
ACTOR Future<Void> dataDistribution(Reference<DataDistributorData> self,
                                    PromiseStream<GetMetricsListRequest> getShardMetricsList,
                                    PromiseStream<DistributorSplitRangeRequest> manualShardSplit,
                                    const DDEnabledState* ddEnabledState) {
	state double lastLimited = 0;
	self->addActor.send(monitorBatchLimitedTime(self->dbInfo, &lastLimited));

	// self->cx = openDBOnServer(self->dbInfo, TaskPriority::DataDistributionLaunch, LockAware::True);
	// state Database& cx = self->cx;
	self->cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;

	// cx->setOption( FDBDatabaseOptions::LOCATION_CACHE_SIZE, StringRef((uint8_t*)
	// &SERVER_KNOBS->DD_LOCATION_CACHE_SIZE, 8) ); ASSERT( cx->locationCacheSize ==
	// SERVER_KNOBS->DD_LOCATION_CACHE_SIZE
	// );

	// wait(debugCheckCoalescing(cx));
	state std::vector<Optional<Key>> primaryDcId;
	state std::vector<Optional<Key>> remoteDcIds;
	state DatabaseConfiguration configuration;
	state Reference<InitialDataDistribution> initData;
	// state MoveKeysLock& lock = self->lock;
	state Reference<DDTeamCollection> primaryTeamCollection;
	state Reference<DDTeamCollection> remoteTeamCollection;
	state bool trackerCancelled;
	loop {
		trackerCancelled = false;

		// Stored outside of data distribution tracker to avoid slow tasks
		// when tracker is cancelled
		state KeyRangeMap<ShardTrackedData> shards;
		state Promise<UID> removeFailedServer;
		try {
			loop {
				TraceEvent("DDInitTakingMoveKeysLock", self->ddId).log();
				MoveKeysLock lock_ = wait(takeMoveKeysLock(self->cx, self->ddId));
				self->lock = lock_;
				TraceEvent("DDInitTookMoveKeysLock", self->ddId).log();

				DatabaseConfiguration configuration_ = wait(getDatabaseConfiguration(self->cx));
				configuration = configuration_;
				primaryDcId.clear();
				remoteDcIds.clear();
				const std::vector<RegionInfo>& regions = configuration.regions;
				if (configuration.regions.size() > 0) {
					primaryDcId.push_back(regions[0].dcId);
				}
				if (configuration.regions.size() > 1) {
					remoteDcIds.push_back(regions[1].dcId);
				}

				TraceEvent("DDInitGotConfiguration", self->ddId).detail("Conf", configuration.toString());

				state Transaction tr(self->cx);
				loop {
					try {
						tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

						RangeResult replicaKeys = wait(tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY));

						for (auto& kv : replicaKeys) {
							auto dcId = decodeDatacenterReplicasKey(kv.key);
							auto replicas = decodeDatacenterReplicasValue(kv.value);
							if ((primaryDcId.size() && primaryDcId[0] == dcId) ||
							    (remoteDcIds.size() && remoteDcIds[0] == dcId && configuration.usableRegions > 1)) {
								if (replicas > configuration.storageTeamSize) {
									tr.set(kv.key, datacenterReplicasValue(configuration.storageTeamSize));
								}
							} else {
								tr.clear(kv.key);
							}
						}

						wait(tr.commit());
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				TraceEvent("DDInitUpdatedReplicaKeys", self->ddId).log();
				Reference<InitialDataDistribution> initData_ = wait(getInitialDataDistribution(
				    self->cx,
				    self->ddId,
				    self->lock,
				    configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>(),
				    ddEnabledState));
				initData = initData_;
				if (initData->shards.size() > 1) {
					TraceEvent("DDInitGotInitialDD", self->ddId)
					    .detail("B", initData->shards.end()[-2].key)
					    .detail("E", initData->shards.end()[-1].key)
					    .detail("Src", describe(initData->shards.end()[-2].primarySrc))
					    .detail("Dest", describe(initData->shards.end()[-2].primaryDest))
					    .trackLatest(self->initialDDEventHolder->trackingKey);
				} else {
					TraceEvent("DDInitGotInitialDD", self->ddId)
					    .detail("B", "")
					    .detail("E", "")
					    .detail("Src", "[no items]")
					    .detail("Dest", "[no items]")
					    .trackLatest(self->initialDDEventHolder->trackingKey);
				}

				if (initData->mode && ddEnabledState->isDDEnabled()) {
					// mode may be set true by system operator using fdbcli and isDDEnabled() set to true
					break;
				}
				TraceEvent("DataDistributionDisabled", self->ddId).log();

				TraceEvent("MovingData", self->ddId)
				    .detail("InFlight", 0)
				    .detail("InQueue", 0)
				    .detail("AverageShardSize", -1)
				    .detail("UnhealthyRelocations", 0)
				    .detail("HighestPriority", 0)
				    .detail("BytesWritten", 0)
				    .detail("PriorityRecoverMove", 0)
				    .detail("PriorityRebalanceUnderutilizedTeam", 0)
				    .detail("PriorityRebalannceOverutilizedTeam", 0)
				    .detail("PriorityTeamHealthy", 0)
				    .detail("PriorityTeamContainsUndesiredServer", 0)
				    .detail("PriorityTeamRedundant", 0)
				    .detail("PriorityMergeShard", 0)
				    .detail("PriorityTeamUnhealthy", 0)
				    .detail("PriorityTeam2Left", 0)
				    .detail("PriorityTeam1Left", 0)
				    .detail("PriorityTeam0Left", 0)
				    .detail("PrioritySplitShard", 0)
				    .trackLatest(self->movingDataEventHolder->trackingKey);

				TraceEvent("TotalDataInFlight", self->ddId)
				    .detail("Primary", true)
				    .detail("TotalBytes", 0)
				    .detail("UnhealthyServers", 0)
				    .detail("HighestPriority", 0)
				    .trackLatest(self->totalDataInFlightEventHolder->trackingKey);
				TraceEvent("TotalDataInFlight", self->ddId)
				    .detail("Primary", false)
				    .detail("TotalBytes", 0)
				    .detail("UnhealthyServers", 0)
				    .detail("HighestPriority", configuration.usableRegions > 1 ? 0 : -1)
				    .trackLatest(self->totalDataInFlightRemoteEventHolder->trackingKey);

				wait(waitForDataDistributionEnabled(self->cx, ddEnabledState));
				TraceEvent("DataDistributionEnabled").log();
			}

			// When/If this assertion fails, Evan owes Ben a pat on the back for his foresight
			ASSERT(configuration.storageTeamSize > 0);

			state PromiseStream<RelocateShard> output;
			state PromiseStream<RelocateShard> input;
			state PromiseStream<Promise<int64_t>> getAverageShardBytes;
			state PromiseStream<ServerTeamInfo> triggerSplitForStorageQueueTooLong;
			state PromiseStream<Promise<int>> getUnhealthyRelocationCount;
			state PromiseStream<GetMetricsRequest> getShardMetrics;
			state Reference<AsyncVar<bool>> processingUnhealthy(new AsyncVar<bool>(false));
			state Reference<AsyncVar<bool>> processingWiggle(new AsyncVar<bool>(false));
			state Promise<Void> readyToStart;
			state Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure(new ShardsAffectedByTeamFailure);

			state int shard = 0;
			for (; shard < initData->shards.size() - 1; shard++) {
				KeyRangeRef keys = KeyRangeRef(initData->shards[shard].key, initData->shards[shard + 1].key);
				shardsAffectedByTeamFailure->defineShard(keys);
				std::vector<ShardsAffectedByTeamFailure::Team> teams;
				teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].primarySrc, true));
				if (configuration.usableRegions > 1) {
					teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].remoteSrc, false));
				}
				if (g_network->isSimulated()) {
					TraceEvent("DDInitShard")
					    .detail("Keys", keys)
					    .detail("PrimarySrc", describe(initData->shards[shard].primarySrc))
					    .detail("RemoteSrc", describe(initData->shards[shard].remoteSrc))
					    .detail("PrimaryDest", describe(initData->shards[shard].primaryDest))
					    .detail("RemoteDest", describe(initData->shards[shard].remoteDest));
				}

				shardsAffectedByTeamFailure->moveShard(keys, teams);
				if (initData->shards[shard].hasDest) {
					// This shard is already in flight.  Ideally we should use dest in ShardsAffectedByTeamFailure and
					// generate a dataDistributionRelocator directly in DataDistributionQueue to track it, but it's
					// easier to just (with low priority) schedule it for movement.
					bool unhealthy = initData->shards[shard].primarySrc.size() != configuration.storageTeamSize;
					if (!unhealthy && configuration.usableRegions > 1) {
						unhealthy = initData->shards[shard].remoteSrc.size() != configuration.storageTeamSize;
					}
					output.send(RelocateShard(
					    keys, unhealthy ? SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY : SERVER_KNOBS->PRIORITY_RECOVER_MOVE));
				}
				wait(yield(TaskPriority::DataDistribution));
			}

			std::vector<TeamCollectionInterface> tcis;

			Reference<AsyncVar<bool>> anyZeroHealthyTeams;
			std::vector<Reference<AsyncVar<bool>>> zeroHealthyTeams;
			tcis.push_back(TeamCollectionInterface());
			zeroHealthyTeams.push_back(makeReference<AsyncVar<bool>>(true));
			int storageTeamSize = configuration.storageTeamSize;

			std::vector<Future<Void>> actors;
			if (configuration.usableRegions > 1) {
				tcis.push_back(TeamCollectionInterface());
				storageTeamSize = 2 * configuration.storageTeamSize;

				zeroHealthyTeams.push_back(makeReference<AsyncVar<bool>>(true));
				anyZeroHealthyTeams = makeReference<AsyncVar<bool>>(true);
				actors.push_back(anyTrue(zeroHealthyTeams, anyZeroHealthyTeams));
			} else {
				anyZeroHealthyTeams = zeroHealthyTeams[0];
			}

			actors.push_back(pollMoveKeysLock(self->cx, self->lock, ddEnabledState));
			actors.push_back(reportErrorsExcept(dataDistributionTracker(initData,
			                                                            self->cx,
			                                                            output,
			                                                            shardsAffectedByTeamFailure,
			                                                            getShardMetrics,
			                                                            getShardMetricsList,
			                                                            manualShardSplit,
			                                                            getAverageShardBytes.getFuture(),
			                                                            triggerSplitForStorageQueueTooLong.getFuture(),
			                                                            readyToStart,
			                                                            anyZeroHealthyTeams,
			                                                            self->ddId,
			                                                            &shards,
			                                                            &trackerCancelled),
			                                    "DDTracker",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));
			actors.push_back(reportErrorsExcept(dataDistributionQueue(self->cx,
			                                                          output,
			                                                          input.getFuture(),
			                                                          getShardMetrics,
			                                                          processingUnhealthy,
			                                                          processingWiggle,
			                                                          tcis,
			                                                          shardsAffectedByTeamFailure,
			                                                          self->lock,
			                                                          getAverageShardBytes,
			                                                          getUnhealthyRelocationCount.getFuture(),
			                                                          self->ddId,
			                                                          storageTeamSize,
			                                                          configuration.storageTeamSize,
			                                                          &lastLimited,
			                                                          ddEnabledState),
			                                    "DDQueue",
			                                    self->ddId,
			                                    &normalDDQueueErrors()));

			std::vector<DDTeamCollection*> teamCollectionsPtrs;
			primaryTeamCollection = makeReference<DDTeamCollection>(
			    self->cx,
			    self->ddId,
			    self->lock,
			    output,
			    shardsAffectedByTeamFailure,
			    configuration,
			    primaryDcId,
			    configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>(),
			    readyToStart.getFuture(),
			    zeroHealthyTeams[0],
			    IsPrimary::True,
			    processingUnhealthy,
			    processingWiggle,
			    getShardMetrics,
			    removeFailedServer,
			    getUnhealthyRelocationCount,
			    getAverageShardBytes,
			    triggerSplitForStorageQueueTooLong);
			teamCollectionsPtrs.push_back(primaryTeamCollection.getPtr());
			auto recruitStorage = IAsyncListener<RequestStream<RecruitStorageRequest>>::create(
			    self->dbInfo, [](auto const& info) { return info.clusterInterface.recruitStorage; });
			if (configuration.usableRegions > 1) {
				remoteTeamCollection =
				    makeReference<DDTeamCollection>(self->cx,
				                                    self->ddId,
				                                    self->lock,
				                                    output,
				                                    shardsAffectedByTeamFailure,
				                                    configuration,
				                                    remoteDcIds,
				                                    Optional<std::vector<Optional<Key>>>(),
				                                    readyToStart.getFuture() && remoteRecovered(self->dbInfo),
				                                    zeroHealthyTeams[1],
				                                    IsPrimary::False,
				                                    processingUnhealthy,
				                                    processingWiggle,
				                                    getShardMetrics,
				                                    removeFailedServer,
				                                    getUnhealthyRelocationCount,
				                                    getAverageShardBytes,
				                                    triggerSplitForStorageQueueTooLong);
				teamCollectionsPtrs.push_back(remoteTeamCollection.getPtr());
				remoteTeamCollection->teamCollections = teamCollectionsPtrs;
				actors.push_back(reportErrorsExcept(
				    DDTeamCollection::run(remoteTeamCollection, initData, tcis[1], recruitStorage, *ddEnabledState),
				    "DDTeamCollectionSecondary",
				    self->ddId,
				    &normalDDQueueErrors()));
				actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(remoteTeamCollection));
			}
			primaryTeamCollection->teamCollections = teamCollectionsPtrs;
			self->teamCollection = primaryTeamCollection.getPtr();
			actors.push_back(reportErrorsExcept(
			    DDTeamCollection::run(primaryTeamCollection, initData, tcis[0], recruitStorage, *ddEnabledState),
			    "DDTeamCollectionPrimary",
			    self->ddId,
			    &normalDDQueueErrors()));

			actors.push_back(DDTeamCollection::printSnapshotTeamsInfo(primaryTeamCollection));
			actors.push_back(yieldPromiseStream(output.getFuture(), input));
			self->initialized = true;

			wait(waitForAll(actors));
			return Void();
		} catch (Error& e) {
			trackerCancelled = true;
			state Error err = e;
			TraceEvent("DataDistributorDestroyTeamCollections").error(e);
			state std::vector<UID> teamForDroppedRange;
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				// Choose a random healthy team to host the to-be-dropped range.
				const UID serverID = removeFailedServer.getFuture().get();
				std::vector<UID> pTeam = primaryTeamCollection->getRandomHealthyTeam(serverID);
				teamForDroppedRange.insert(teamForDroppedRange.end(), pTeam.begin(), pTeam.end());
				if (configuration.usableRegions > 1) {
					std::vector<UID> rTeam = remoteTeamCollection->getRandomHealthyTeam(serverID);
					teamForDroppedRange.insert(teamForDroppedRange.end(), rTeam.begin(), rTeam.end());
				}
			}
			self->teamCollection = nullptr;
			primaryTeamCollection = Reference<DDTeamCollection>();
			remoteTeamCollection = Reference<DDTeamCollection>();
			if (err.code() == error_code_actor_cancelled) {
				// When cancelled, we cannot clear asyncronously because
				// this will result in invalid memory access. This should only
				// be an issue in simulation.
				if (!g_network->isSimulated()) {
					TraceEvent(SevWarnAlways, "DataDistributorCancelled");
				}
				shards.clear();
				throw e;
			} else {
				wait(shards.clearAsync());
			}
			TraceEvent("DataDistributorTeamCollectionsDestroyed").error(err);
			if (removeFailedServer.getFuture().isReady() && !removeFailedServer.getFuture().isError()) {
				TraceEvent("RemoveFailedServer", removeFailedServer.getFuture().get()).error(err);
				wait(removeKeysFromFailedServer(
				    self->cx, removeFailedServer.getFuture().get(), teamForDroppedRange, self->lock, ddEnabledState));
				Optional<UID> tssPairID;
				wait(removeStorageServer(
				    self->cx, removeFailedServer.getFuture().get(), tssPairID, self->lock, ddEnabledState));
			} else {
				if (err.code() != error_code_movekeys_conflict) {
					throw err;
				}

				bool ddEnabled = wait(isDataDistributionEnabled(self->cx, ddEnabledState));
				TraceEvent("DataDistributionMoveKeysConflict").error(err).detail("DataDistributionEnabled", ddEnabled);
				if (ddEnabled) {
					throw err;
				}
			}
		}
	}
}

static std::set<int> const& normalDataDistributorErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_worker_removed);
		s.insert(error_code_broken_promise);
		s.insert(error_code_actor_cancelled);
		s.insert(error_code_please_reboot);
		s.insert(error_code_movekeys_conflict);
	}
	return s;
}

ACTOR template <class Req>
Future<Void> sendSnapReq(RequestStream<Req> stream, Req req, Error e) {
	ErrorOr<REPLY_TYPE(Req)> reply = wait(stream.tryGetReply(req));
	if (reply.isError()) {
		TraceEvent("SnapDataDistributor_ReqError")
		    .errorUnsuppressed(reply.getError())
		    .detail("ConvertedErrorType", e.what())
		    .detail("Peer", stream.getEndpoint().getPrimaryAddress());
		throw e;
	}
	return Void();
}

ACTOR template <class Req>
Future<ErrorOr<Void>> trySendSnapReq(RequestStream<Req> stream, Req req) {
	ErrorOr<REPLY_TYPE(Req)> reply = wait(stream.tryGetReply(req));
	if (reply.isError()) {
		TraceEvent("SnapDataDistributor_ReqError")
		    .errorUnsuppressed(reply.getError())
		    .detail("Peer", stream.getEndpoint().getPrimaryAddress());
		return ErrorOr<Void>(reply.getError());
	}
	return ErrorOr<Void>(Void());
}

ACTOR static Future<Void> waitForMost(std::vector<Future<ErrorOr<Void>>> futures,
                                      int faultTolerance,
                                      Error e,
                                      double waitMultiplierForSlowFutures = 1.0) {
	state std::vector<Future<bool>> successFutures;
	state double startTime = now();
	successFutures.reserve(futures.size());
	for (const auto& future : futures) {
		successFutures.push_back(fmap([](auto const& result) { return result.present(); }, future));
	}
	bool success = wait(quorumEqualsTrue(successFutures, successFutures.size() - faultTolerance));
	if (!success) {
		throw e;
	}
	wait(delay((now() - startTime) * waitMultiplierForSlowFutures) || waitForAll(successFutures));
	return Void();
}

ACTOR Future<Void> ddSnapCreateCore(DistributorSnapRequest snapReq, Reference<AsyncVar<ServerDBInfo> const> db) {
	state Database cx = openDBOnServer(db, TaskPriority::DefaultDelay, LockAware::True);

	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			TraceEvent("SnapDataDistributor_WriteFlagAttempt")
			    .detail("SnapPayload", snapReq.snapPayload)
			    .detail("SnapUID", snapReq.snapUID);
			tr.set(writeRecoveryKey, writeRecoveryKeyTrue);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			TraceEvent("SnapDataDistributor_WriteFlagError").error(e);
			wait(tr.onError(e));
		}
	}
	TraceEvent("SnapDataDistributor_SnapReqEnter")
	    .detail("SnapPayload", snapReq.snapPayload)
	    .detail("SnapUID", snapReq.snapUID);
	try {
		// disable tlog pop on local tlog nodes
		state std::vector<TLogInterface> tlogs = db->get().logSystemConfig.allLocalLogs(false);
		std::vector<Future<Void>> disablePops;
		disablePops.reserve(tlogs.size());
		for (const auto& tlog : tlogs) {
			disablePops.push_back(sendSnapReq(
			    tlog.disablePopRequest, TLogDisablePopRequest{ snapReq.snapUID }, snap_disable_tlog_pop_failed()));
		}
		wait(waitForAll(disablePops));

		TraceEvent("SnapDataDistributor_AfterDisableTLogPop")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		// snap local storage nodes
		// TODO: Atomically read  configuration and storage worker list in a single transaction
		state DatabaseConfiguration configuration = wait(getDatabaseConfiguration(cx));
		std::pair<std::vector<WorkerInterface>, int> storageWorkersAndFailures =
		    wait(transformErrors(getStorageWorkers(cx, db, true /* localOnly */), snap_storage_failed()));
		const auto& [storageWorkers, storageFailures] = storageWorkersAndFailures;
		auto const storageFaultTolerance =
		    std::min(static_cast<int>(SERVER_KNOBS->MAX_STORAGE_SNAPSHOT_FAULT_TOLERANCE),
		             configuration.storageTeamSize - 1) -
		    storageFailures;
		if (storageFaultTolerance < 0) {
			TEST(true); // Too many failed storage servers to complete snapshot
			throw snap_storage_failed();
		}
		TraceEvent("SnapDataDistributor_GotStorageWorkers")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		std::vector<Future<ErrorOr<Void>>> storageSnapReqs;
		storageSnapReqs.reserve(storageWorkers.size());
		for (const auto& worker : storageWorkers) {
			storageSnapReqs.push_back(trySendSnapReq(
			    worker.workerSnapReq, WorkerSnapRequest(snapReq.snapPayload, snapReq.snapUID, "storage"_sr)));
		}
		wait(waitForMost(storageSnapReqs, storageFaultTolerance, snap_storage_failed()));

		TraceEvent("SnapDataDistributor_AfterSnapStorage")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		// snap local tlog nodes
		std::vector<Future<Void>> tLogSnapReqs;
		tLogSnapReqs.reserve(tlogs.size());
		for (const auto& tlog : tlogs) {
			tLogSnapReqs.push_back(sendSnapReq(tlog.snapRequest,
			                                   TLogSnapRequest{ snapReq.snapPayload, snapReq.snapUID, "tlog"_sr },
			                                   snap_tlog_failed()));
		}
		wait(waitForAll(tLogSnapReqs));

		TraceEvent("SnapDataDistributor_AfterTLogStorage")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		// enable tlog pop on local tlog nodes
		std::vector<Future<Void>> enablePops;
		enablePops.reserve(tlogs.size());
		for (const auto& tlog : tlogs) {
			enablePops.push_back(sendSnapReq(
			    tlog.enablePopRequest, TLogEnablePopRequest{ snapReq.snapUID }, snap_enable_tlog_pop_failed()));
		}
		wait(waitForAll(enablePops));

		TraceEvent("SnapDataDistributor_AfterEnableTLogPops")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		// snap the coordinators
		std::vector<WorkerInterface> coordWorkers = wait(getCoordWorkers(cx, db));
		TraceEvent("SnapDataDistributor_GotCoordWorkers")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		std::vector<Future<ErrorOr<Void>>> coordSnapReqs;
		coordSnapReqs.reserve(coordWorkers.size());
		for (const auto& worker : coordWorkers) {
			coordSnapReqs.push_back(trySendSnapReq(
			    worker.workerSnapReq, WorkerSnapRequest(snapReq.snapPayload, snapReq.snapUID, "coord"_sr)));
		}
		auto const coordFaultTolerance = std::min<int>(std::max<int>(0, coordSnapReqs.size() / 2 - 1),
		                                               SERVER_KNOBS->MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE);
		wait(waitForMost(coordSnapReqs, coordFaultTolerance, snap_coord_failed()));
		TraceEvent("SnapDataDistributor_AfterSnapCoords")
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		tr.reset();
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				TraceEvent("SnapDataDistributor_ClearFlagAttempt")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				tr.clear(writeRecoveryKey);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent("SnapDataDistributor_ClearFlagError").error(e);
				wait(tr.onError(e));
			}
		}
	} catch (Error& err) {
		state Error e = err;
		TraceEvent("SnapDataDistributor_SnapReqExit")
		    .errorUnsuppressed(e)
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		if (e.code() == error_code_snap_storage_failed || e.code() == error_code_snap_tlog_failed ||
		    e.code() == error_code_operation_cancelled || e.code() == error_code_snap_disable_tlog_pop_failed) {
			// enable tlog pop on local tlog nodes
			std::vector<TLogInterface> tlogs = db->get().logSystemConfig.allLocalLogs(false);
			try {
				std::vector<Future<Void>> enablePops;
				enablePops.reserve(tlogs.size());
				for (const auto& tlog : tlogs) {
					enablePops.push_back(transformErrors(
					    throwErrorOr(tlog.enablePopRequest.tryGetReply(TLogEnablePopRequest(snapReq.snapUID))),
					    snap_enable_tlog_pop_failed()));
				}
				wait(waitForAll(enablePops));
			} catch (Error& error) {
				TraceEvent(SevDebug, "IgnoreEnableTLogPopFailure").log();
			}
		}
		throw e;
	}
	return Void();
}

ACTOR Future<Void> ddSnapCreate(DistributorSnapRequest snapReq,
                                Reference<AsyncVar<ServerDBInfo> const> db,
                                DDEnabledState* ddEnabledState) {
	state Future<Void> dbInfoChange = db->onChange();
	if (!ddEnabledState->setDDEnabled(false, snapReq.snapUID)) {
		// disable DD before doing snapCreate, if previous snap req has already disabled DD then this operation fails
		// here
		TraceEvent("SnapDDSetDDEnabledFailedInMemoryCheck").log();
		snapReq.reply.sendError(operation_failed());
		return Void();
	}
	double delayTime = g_network->isSimulated() ? 70.0 : SERVER_KNOBS->SNAP_CREATE_MAX_TIMEOUT;
	try {
		choose {
			when(wait(dbInfoChange)) {
				TraceEvent("SnapDDCreateDBInfoChanged")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				snapReq.reply.sendError(snap_with_recovery_unsupported());
			}
			when(wait(ddSnapCreateCore(snapReq, db))) {
				TraceEvent("SnapDDCreateSuccess")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				snapReq.reply.send(Void());
			}
			when(wait(delay(delayTime))) {
				TraceEvent("SnapDDCreateTimedOut")
				    .detail("SnapPayload", snapReq.snapPayload)
				    .detail("SnapUID", snapReq.snapUID);
				snapReq.reply.sendError(timed_out());
			}
		}
	} catch (Error& e) {
		TraceEvent("SnapDDCreateError")
		    .errorUnsuppressed(e)
		    .detail("SnapPayload", snapReq.snapPayload)
		    .detail("SnapUID", snapReq.snapUID);
		if (e.code() != error_code_operation_cancelled) {
			snapReq.reply.sendError(e);
		} else {
			// enable DD should always succeed
			bool success = ddEnabledState->setDDEnabled(true, snapReq.snapUID);
			ASSERT(success);
			throw e;
		}
	}
	// enable DD should always succeed
	bool success = ddEnabledState->setDDEnabled(true, snapReq.snapUID);
	ASSERT(success);
	return Void();
}

ACTOR Future<Void> ddExclusionSafetyCheck(DistributorExclusionSafetyCheckRequest req,
                                          Reference<DataDistributorData> self,
                                          Database cx) {
	TraceEvent("DDExclusionSafetyCheckBegin", self->ddId).log();
	std::vector<StorageServerInterface> ssis = wait(getStorageServers(cx));
	DistributorExclusionSafetyCheckReply reply(true);
	if (!self->teamCollection) {
		TraceEvent("DDExclusionSafetyCheckTeamCollectionInvalid", self->ddId).log();
		reply.safe = false;
		req.reply.send(reply);
		return Void();
	}
	// If there is only 1 team, unsafe to mark failed: team building can get stuck due to lack of servers left
	if (self->teamCollection->teams.size() <= 1) {
		TraceEvent("DDExclusionSafetyCheckNotEnoughTeams", self->ddId).log();
		reply.safe = false;
		req.reply.send(reply);
		return Void();
	}
	std::vector<UID> excludeServerIDs;
	// Go through storage server interfaces and translate Address -> server ID (UID)
	for (const AddressExclusion& excl : req.exclusions) {
		for (const auto& ssi : ssis) {
			if (excl.excludes(ssi.address()) ||
			    (ssi.secondaryAddress().present() && excl.excludes(ssi.secondaryAddress().get()))) {
				excludeServerIDs.push_back(ssi.id());
			}
		}
	}
	reply.safe = self->teamCollection->exclusionSafetyCheck(excludeServerIDs);
	TraceEvent("DDExclusionSafetyCheckFinish", self->ddId).log();
	req.reply.send(reply);
	return Void();
}

ACTOR Future<Void> waitFailCacheServer(Database* db, StorageServerInterface ssi) {
	state Transaction tr(*db);
	state Key key = storageCacheServerKey(ssi.id());
	wait(waitFailureClient(ssi.waitFailure));
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr.addReadConflictRange(storageCacheServerKeys);
			tr.clear(key);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> cacheServerWatcher(Database* db) {
	state Transaction tr(*db);
	state ActorCollection actors(false);
	state std::set<UID> knownCaches;
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			RangeResult range = wait(tr.getRange(storageCacheServerKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!range.more);
			std::set<UID> caches;
			for (auto& kv : range) {
				UID id;
				BinaryReader reader{ kv.key.removePrefix(storageCacheServersPrefix), Unversioned() };
				reader >> id;
				caches.insert(id);
				if (knownCaches.find(id) == knownCaches.end()) {
					StorageServerInterface ssi;
					BinaryReader reader{ kv.value, IncludeVersion() };
					reader >> ssi;
					actors.add(waitFailCacheServer(db, ssi));
				}
			}
			knownCaches = std::move(caches);
			tr.reset();
			wait(delay(5.0) || actors.getResult());
			ASSERT(!actors.getResult().isReady());
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

static int64_t getMedianShardSize(VectorRef<DDMetricsRef> metricVec) {
	std::nth_element(metricVec.begin(),
	                 metricVec.begin() + metricVec.size() / 2,
	                 metricVec.end(),
	                 [](const DDMetricsRef& d1, const DDMetricsRef& d2) { return d1.shardBytes < d2.shardBytes; });
	return metricVec[metricVec.size() / 2].shardBytes;
}

ACTOR Future<Void> ddGetMetrics(GetDataDistributorMetricsRequest req,
                                PromiseStream<GetMetricsListRequest> getShardMetricsList) {
	ErrorOr<Standalone<VectorRef<DDMetricsRef>>> result = wait(
	    errorOr(brokenPromiseToNever(getShardMetricsList.getReply(GetMetricsListRequest(req.keys, req.shardLimit)))));

	if (result.isError()) {
		req.reply.sendError(result.getError());
	} else {
		GetDataDistributorMetricsReply rep;
		if (!req.midOnly) {
			rep.storageMetricsList = result.get();
		} else {
			auto& metricVec = result.get();
			if (metricVec.empty())
				rep.midShardSize = 0;
			else {
				rep.midShardSize = getMedianShardSize(metricVec.contents());
			}
		}
		req.reply.send(rep);
	}

	return Void();
}

// Handling audit requests
// For each request, launch audit storage and reply to CC with following three replies:
// (1) auditID: reply auditID when the audit is successfully launch
// (2) error_code_audit_storage_exceeded_request_limit: reply this error when dd
// already has a running auditStorage
// (3) error_code_audit_storage_failed: reply this error when: 1. the retry time exceeds the maximum;
// 2. failed to persist new audit state; 3. DD is cancelled during persisting new audit state
// For 1 and 2, we believe no new audit is persisted; For 3, we do not know whether a new
// audit is persisted.
ACTOR Future<Void> auditStorage(Reference<DataDistributorData> self, TriggerAuditRequest req) {
	state FlowLock::Releaser holder;
	if (req.getType() == AuditType::ValidateHA) {
		// wait(self->auditStorageHaLaunchingLock.take(TaskPriority::DefaultYield));
		// holder = FlowLock::Releaser(self->auditStorageHaLaunchingLock);
		req.reply.sendError(not_implemented());
	} else if (req.getType() == AuditType::ValidateReplica) {
		wait(self->auditStorageReplicaLaunchingLock.take(TaskPriority::DefaultYield));
		holder = FlowLock::Releaser(self->auditStorageReplicaLaunchingLock);
	} else if (req.getType() == AuditType::ValidateLocationMetadata) {
		// wait(self->auditStorageLocationMetadataLaunchingLock.take(TaskPriority::DefaultYield));
		// holder = FlowLock::Releaser(self->auditStorageLocationMetadataLaunchingLock);
		req.reply.sendError(not_implemented());
	} else if (req.getType() == AuditType::ValidateStorageServerShard) {
		// wait(self->auditStorageSsShardLaunchingLock.take(TaskPriority::DefaultYield));
		// holder = FlowLock::Releaser(self->auditStorageSsShardLaunchingLock);
		req.reply.sendError(not_implemented());
	} else {
		req.reply.sendError(not_implemented());
		return Void();
	}

	if (req.range.empty()) {
		req.reply.sendError(audit_storage_failed());
		return Void();
	}

	state int retryCount = 0;
	loop {
		try {
			TraceEvent(SevInfo, "DDAuditStorageStart", self->ddId)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range);
			UID auditID = wait(launchAudit(self, req.range, req.getType(), req.engineType));
			req.reply.send(auditID);
			TraceEvent(SevVerbose, "DDAuditStorageReply", self->ddId)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range)
			    .detail("AuditID", auditID);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "DDAuditStorageError", self->ddId)
			    .errorUnsuppressed(e)
			    .detail("RetryCount", retryCount)
			    .detail("AuditType", req.getType())
			    .detail("KeyValueStoreType", req.engineType.toString())
			    .detail("Range", req.range);
			if (e.code() == error_code_operation_failed && g_network->isSimulated()) {
				throw audit_storage_failed(); // to trigger dd restart
			} else if (e.code() == error_code_audit_storage_exceeded_request_limit) {
				req.reply.sendError(audit_storage_exceeded_request_limit());
			} else if (e.code() == error_code_persist_new_audit_metadata_error) {
				req.reply.sendError(audit_storage_failed());
			} else if (retryCount < SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
				retryCount++;
				wait(delay(0.1));
				continue;
			} else {
				req.reply.sendError(audit_storage_failed());
			}
		}
		break;
	}
	return Void();
}

ACTOR Future<std::vector<DDRangeLocations>> getSourceServerInterfacesForRange(Database cx, KeyRangeRef range) {
	state std::vector<DDRangeLocations> res;
	state Transaction tr(cx);
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	loop {
		res.clear();
		try {
			state RangeResult shards = wait(krmGetRanges(&tr,
			                                             keyServersPrefix,
			                                             range,
			                                             CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                             CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			ASSERT(!shards.empty());

			state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

			state int i = 0;
			for (i = 0; i < shards.size() - 1; ++i) {
				state std::vector<UID> src;
				std::vector<UID> dest;
				decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest);

				std::vector<Future<Optional<Value>>> serverListEntries;
				for (int j = 0; j < src.size(); ++j) {
					serverListEntries.push_back(tr.get(serverListKeyFor(src[j])));
				}
				std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
				DDRangeLocations current(KeyRangeRef(shards[i].key, shards[i + 1].key));
				for (int j = 0; j < serverListValues.size(); ++j) {
					if (!serverListValues[j].present()) {
						TraceEvent(SevWarnAlways, "GetSourceServerInterfacesMissing")
						    .detail("StorageServer", src[j])
						    .detail("Range", KeyRangeRef(shards[i].key, shards[i + 1].key));
						continue;
					}
					StorageServerInterface ssi = decodeServerListValue(serverListValues[j].get());
					current.servers[ssi.locality.describeDcId()].push_back(ssi);
				}
				res.push_back(current);
			}
			break;
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "GetSourceServerInterfacesError").errorUnsuppressed(e).detail("Range", range);
			wait(tr.onError(e));
		}
	}

	return res;
}

ACTOR Future<std::unordered_map<UID, KeyValueStoreType>> getStorageType(
    std::vector<StorageServerInterface> storageServers) {
	state std::vector<Future<ErrorOr<KeyValueStoreType>>> storageTypeFutures;
	state std::unordered_map<UID, KeyValueStoreType> res;

	try {
		for (int i = 0; i < storageServers.size(); i++) {
			ReplyPromise<KeyValueStoreType> typeReply;
			storageTypeFutures.push_back(
			    storageServers[i].getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));
		}
		wait(waitForAll(storageTypeFutures));

		for (int i = 0; i < storageServers.size(); i++) {
			ErrorOr<KeyValueStoreType> reply = storageTypeFutures[i].get();
			if (!reply.present()) {
				TraceEvent(SevWarn, "AuditStorageFailedToGetStorageType")
				    .error(reply.getError())
				    .detail("StorageServer", storageServers[i].id())
				    .detail("IsTSS", storageServers[i].isTss() ? "True" : "False");
			} else {
				res[storageServers[i].id()] = reply.get();
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent("AuditStorageErrorGetStorageType").errorUnsuppressed(e);
		res.clear();
	}

	return res;
}

// Partition the input range into multiple subranges according to the range ownership, and
// schedule ha/replica audit tasks of each subrange on the server which owns the subrange
// Automatically retry until complete or timed out
ACTOR Future<Void> scheduleAuditOnRange(Reference<DataDistributorData> self,
                                        std::shared_ptr<DDAudit> audit,
                                        KeyRange rangeToSchedule) {
	state const AuditType auditType = audit->coreState.getType();
	ASSERT(auditType == AuditType::ValidateReplica);

	TraceEvent(SevInfo, "DDScheduleAuditOnRangeBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditRange", audit->coreState.range)
	    .detail("RangeToSchedule", rangeToSchedule)
	    .detail("AuditType", auditType)
	    .detail("RemainingBudget", audit->remainingBudgetForAuditTasks.get());

	state Key currentRangeToScheduleBegin = rangeToSchedule.begin;
	state KeyRange currentRangeToSchedule;
	state int64_t issueDoAuditCount = 0;
	state int64_t numSkippedShards = 0;

	try {
		while (currentRangeToScheduleBegin < rangeToSchedule.end) {
			currentRangeToSchedule = Standalone(KeyRangeRef(currentRangeToScheduleBegin, rangeToSchedule.end));
			state std::vector<DDRangeLocations> rangeLocations =
			    wait(getSourceServerInterfacesForRange(self->cx, currentRangeToSchedule));
			if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
				TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRange", self->ddId)
				    .detail("AuditID", audit->coreState.id)
				    .detail("AuditType", auditType)
				    .detail("RangeToSchedule", rangeToSchedule)
				    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
				    .detail("NumTaskRanges", rangeLocations.size())
				    .detail("RangeLocationsBackKey", rangeLocations.back().range.end);
			}

			// Divide the audit job in to tasks according to KeyServers system mapping
			state int assignedRangeTasks = 0;
			state int rangeLocationIndex = 0;
			for (; rangeLocationIndex < rangeLocations.size(); ++rangeLocationIndex) {
				// For each task, check the progress, and create task request for the unfinished range
				state KeyRange taskRange = rangeLocations[rangeLocationIndex].range;
				if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
					TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeTask", self->ddId)
					    .detail("AuditID", audit->coreState.id)
					    .detail("AuditType", auditType)
					    .detail("RangeToSchedule", rangeToSchedule)
					    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
					    .detail("TaskRange", taskRange);
				}

				state Key taskRangeBegin = taskRange.begin;
				while (taskRangeBegin < taskRange.end) {
					state std::vector<AuditStorageState> auditStates =
					    wait(getAuditStateByRange(self->cx,
					                              auditType,
					                              audit->coreState.id,
					                              KeyRangeRef(taskRangeBegin, taskRange.end)));
					if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
						TraceEvent(SevInfo, "DDScheduleAuditOnRangeSubTask", self->ddId)
						    .detail("AuditID", audit->coreState.id)
						    .detail("AuditType", auditType)
						    .detail("AuditRange", audit->coreState.range)
						    .detail("RangeToSchedule", rangeToSchedule)
						    .detail("CurrentRangeToSchedule", currentRangeToSchedule)
						    .detail("TaskRange", taskRange)
						    .detail("SubTaskBegin", taskRangeBegin)
						    .detail("SubTaskEnd", auditStates.back().range.end)
						    .detail("NumAuditStates", auditStates.size());
					}
					ASSERT(!auditStates.empty());

					state int auditStateIndex = 0;
					for (; auditStateIndex < auditStates.size(); ++auditStateIndex) {
						state AuditPhase phase = auditStates[auditStateIndex].getPhase();
						ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
						if (phase == AuditPhase::Complete) {
							continue;
						} else if (phase == AuditPhase::Error) {
							audit->foundError = true;
							continue;
						}
						// Create audit task for the range where the phase is Invalid which indicates
						// this range has not been audited
						ASSERT(phase == AuditPhase::Invalid);
						state AuditStorageRequest req(
						    audit->coreState.id, auditStates[auditStateIndex].range, auditType);
						state StorageServerInterface targetServer;
						state std::vector<StorageServerInterface> storageServersToCheck;

						// Set req.targetServers and targetServer, which will be
						// used to doAuditOnStorageServer
						if (auditType == AuditType::ValidateReplica) {
							// select a server from primary DC to do audit
							// check all servers from each DC
                            // TODO: Maybe select a remote server, to minimize load on primary servers.
							int dcid = 0;
							for (const auto& [_, dcServers] : rangeLocations[rangeLocationIndex].servers) {
								if (dcid == 0) {
									// in primary DC randomly select a server to do the audit task
									const int idx = deterministicRandom()->randomInt(0, dcServers.size());
									targetServer = dcServers[idx];
								}
								for (int i = 0; i < dcServers.size(); i++) {
									if (dcServers[i].id() == targetServer.id()) {
										ASSERT_WE_THINK(dcid == 0);
									} else {
										req.targetServers.push_back(dcServers[i].id());
									}
									storageServersToCheck.push_back(dcServers[i]);
								}
								dcid++;
							}
							if (storageServersToCheck.size() <= 1) {
								TraceEvent(SevInfo, "DDScheduleAuditOnRangeEnd", self->ddId)
								    .detail("Reason", "Single replica, ignore")
								    .detail("AuditID", audit->coreState.id)
								    .detail("AuditRange", audit->coreState.range)
								    .detail("AuditType", auditType);
								return Void();
							}
						} else {
							UNREACHABLE();
						}

						// Set doAuditOnStorageServer
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						while (audit->remainingBudgetForAuditTasks.get() == 0) {
							wait(audit->remainingBudgetForAuditTasks.onChange());
							ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						}
						audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() - 1);
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
						TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
						    .detail("Loc", "scheduleAuditOnRange1")
						    .detail("Ops", "Decrease")
						    .detail("Val", audit->remainingBudgetForAuditTasks.get())
						    .detail("AuditType", auditType);

						req.ddId = self->ddId; // send this ddid to SS
						// Check if the shard is in any specified storage engine
						// If yes, issue doAuditOnStorageServer
						// Otherwise, persist progress complete
						state bool anySpecifiedEngine = false;
						if (audit->coreState.engineType == KeyValueStoreType::END) {
							// Do not specify any storage engine, so check for all engine types
							anySpecifiedEngine = true;
						} else {
							try {
								std::unordered_map<UID, KeyValueStoreType> storageTypeMapping =
								    wait(getStorageType(storageServersToCheck));
								for (int j = 0; j < storageServersToCheck.size(); j++) {
									auto ssStorageType = storageTypeMapping.find(storageServersToCheck[j].id());
									if (ssStorageType != storageTypeMapping.end()) {
										if (ssStorageType->second == audit->coreState.engineType) {
											anySpecifiedEngine = true;
											break;
										}
									}
								}
							} catch (Error& e) {
								audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
								ASSERT(audit->remainingBudgetForAuditTasks.get() <=
								       SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
								TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
								    .detail("Loc", "scheduleAuditOnRange")
								    .detail("Ops", "Increase")
								    .detail("Val", audit->remainingBudgetForAuditTasks.get())
								    .detail("AuditType", auditType);
								throw e;
							}
						}
						if (!anySpecifiedEngine) {
							numSkippedShards++;
							// audit->actors.add(skipAuditOnRange(self, audit, auditStates[auditStateIndex].range));
						} else {
							issueDoAuditCount++;
							// audit->actors.add(doAuditOnStorageServer(self, audit, targetServer, req));
						}
					}

					taskRangeBegin = auditStates.back().range.end;
					if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
						TraceEvent(SevInfo, "DDScheduleAuditOnRangeSubTaskAssigned", self->ddId)
						    .detail("TaskRange", taskRange)
						    .detail("NextTaskRangeBegin", taskRangeBegin)
						    .detail("BreakRangeEnd", taskRange.end);
					}
				}
				if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
					TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeTaskAssigned", self->ddId);
				}
				++assignedRangeTasks;
				wait(delay(0.1));
			}
			// Proceed to the next range if getSourceServerInterfacesForRange is partially read
			currentRangeToScheduleBegin = rangeLocations.back().range.end;
			if (SERVER_KNOBS->ENABLE_AUDIT_VERBOSE_TRACE) {
				TraceEvent(SevInfo, "DDScheduleAuditOnCurrentRangeAssigned", self->ddId)
				    .detail("AssignedRangeTasks", assignedRangeTasks)
				    .detail("NextCurrentRangeToScheduleBegin", currentRangeToScheduleBegin)
				    .detail("BreakRangeEnd", rangeToSchedule.end)
				    .detail("RangeToSchedule", rangeToSchedule);
			}
		}

		TraceEvent(SevInfo, "DDScheduleAuditOnRangeEnd", self->ddId)
		    .detail("Reason", "End")
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("RangeToSchedule", rangeToSchedule)
		    .detail("AuditType", auditType)
		    .detail("SkippedShardsCountInThisSchedule", numSkippedShards)
		    .detail("IssuedDoAuditCountInThisSchedule", issueDoAuditCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDScheduleAuditOnRangeError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditRange", audit->coreState.range)
		    .detail("RangeToSchedule", rangeToSchedule)
		    .detail("AuditType", auditType)
		    .detail("SkippedShardsCountInThisSchedule", numSkippedShards)
		    .detail("IssuedDoAuditCountInThisSchedule", issueDoAuditCount);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

// Request SS to do the audit
// This actor is the only interface to SS to do the audit for
// all audit types
ACTOR Future<Void> doAuditOnStorageServer(Reference<DataDistributorData> self,
                                          std::shared_ptr<DDAudit> audit,
                                          StorageServerInterface ssi,
                                          AuditStorageRequest req) {
	state AuditType auditType = req.getType();
	ASSERT(auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica ||
	       auditType == AuditType::ValidateStorageServerShard);
	TraceEvent(SevInfo, "DDDoAuditOnStorageServerBegin", self->ddId)
	    .detail("AuditID", req.id)
	    .detail("Range", req.range)
	    .detail("AuditType", auditType)
	    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
	    .detail("StorageServer", ssi.toString())
	    .detail("TargetServers", describe(req.targetServers))
	    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
	    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
	    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);

	try {
		audit->overallIssuedDoAuditCount++;
		ASSERT(req.ddId.isValid());
		ErrorOr<AuditStorageState> vResult = wait(ssi.auditStorage.tryGetReply(req));
		if (vResult.isError()) {
			throw vResult.getError();
		}
		audit->overallCompleteDoAuditCount++;
		TraceEvent(SevInfo, "DDDoAuditOnStorageServerResult", self->ddId)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers))
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditOnStorageServer")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDDoAuditOnStorageServerError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", req.id)
		    .detail("Range", req.range)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", audit->coreState.engineType.toString())
		    .detail("StorageServer", ssi.toString())
		    .detail("TargetServers", describe(req.targetServers))
		    .detail("DDDoAuditTaskIssue", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTaskComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTaskSkip", audit->overallSkippedDoAuditCount);
		audit->remainingBudgetForAuditTasks.set(audit->remainingBudgetForAuditTasks.get() + 1);
		ASSERT(audit->remainingBudgetForAuditTasks.get() <= SERVER_KNOBS->CONCURRENT_AUDIT_TASK_COUNT_MAX);
		TraceEvent(SevDebug, "RemainingBudgetForAuditTasks")
		    .detail("Loc", "doAuditOnStorageServerError")
		    .detail("Ops", "Increase")
		    .detail("Val", audit->remainingBudgetForAuditTasks.get())
		    .detail("AuditType", auditType);
		if (req.getType() == AuditType::ValidateStorageServerShard) {
			throw e; // handled by scheduleAuditStorageShardOnServer
		}
		if (e.code() == error_code_not_implemented || e.code() == error_code_audit_storage_exceeded_request_limit ||
		    e.code() == error_code_audit_storage_cancelled || e.code() == error_code_audit_storage_task_outdated) {
			throw e;
		} else if (e.code() == error_code_audit_storage_error) {
			audit->foundError = true;
		} else if (audit->retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
			throw audit_storage_failed();
		} else {
			ASSERT(req.getType() != AuditType::ValidateStorageServerShard);
			audit->retryCount++;
			audit->actors.add(scheduleAuditOnRange(self, audit, req.range));
		}
	}
	return Void();
}

// Schedule audit task on the input range
ACTOR Future<Void> dispatchAuditStorage(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit) {
	state const AuditType auditType = audit->coreState.getType();
	state const KeyRange range = audit->coreState.range;
	ASSERT(auditType == AuditType::ValidateReplica);
	TraceEvent(SevInfo, "DDDispatchAuditStorageBegin", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("Range", range)
	    .detail("AuditType", auditType);
	state Key begin = range.begin;
	state KeyRange currentRange = range;
	state int64_t completedCount = 0;
	state int64_t totalCount = 0;

	try {
		while (begin < range.end) {
			currentRange = KeyRangeRef(begin, range.end);
			state std::vector<AuditStorageState> auditStates =
			    wait(getAuditStateByRange(self->cx, auditType, audit->coreState.id, currentRange));
			ASSERT(!auditStates.empty());
			begin = auditStates.back().range.end;
			TraceEvent(SevInfo, "DDDispatchAuditStorageDispatch", self->ddId)
			    .detail("AuditID", audit->coreState.id)
			    .detail("Range", range)
			    .detail("CurrentRange", currentRange)
			    .detail("AuditType", auditType)
			    .detail("NextBegin", begin)
			    .detail("NumAuditStates", auditStates.size());
			state int i = 0;
			for (; i < auditStates.size(); i++) {
				state AuditPhase phase = auditStates[i].getPhase();
				ASSERT(phase != AuditPhase::Running && phase != AuditPhase::Failed);
				totalCount++;
				if (phase == AuditPhase::Complete) {
					completedCount++;
				} else if (phase == AuditPhase::Error) {
					completedCount++;
					audit->foundError = true;
				} else {
					ASSERT(phase == AuditPhase::Invalid);
					ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					while (audit->remainingBudgetForAuditTasks.get() == 0) {
						wait(audit->remainingBudgetForAuditTasks.onChange());
						ASSERT(audit->remainingBudgetForAuditTasks.get() >= 0);
					}
					audit->actors.add(scheduleAuditOnRange(self, audit, auditStates[i].range));
				}
			}
			wait(delay(0.1));
		}

		TraceEvent(SevInfo, "DDDispatchAuditStorageEnd", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", range)
		    .detail("AuditType", auditType)
		    .detail("TotalRanges", totalCount)
		    .detail("TotalComplete", completedCount)
		    .detail("CompleteRatio", completedCount * 1.0 / totalCount);

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}

		TraceEvent(SevWarn, "DDDispatchAuditStorageError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditID", audit->coreState.id)
		    .detail("AuditType", auditType);
		audit->auditStorageAnyChildFailed = true;
	}

	return Void();
}

// The entry of starting a series of audit workers
// Decide which dispatch impl according to audit type
void loadAndDispatchAudit(Reference<DataDistributorData> self, std::shared_ptr<DDAudit> audit) {
	TraceEvent(SevInfo, "DDLoadAndDispatchAudit", self->ddId)
	    .detail("AuditID", audit->coreState.id)
	    .detail("AuditType", audit->coreState.getType())
	    .detail("AuditRange", audit->coreState.range);

	if (audit->coreState.getType() == AuditType::ValidateReplica) {
		audit->actors.add(dispatchAuditStorage(self, audit));
	} else {
		throw not_implemented();
	}

	return;
}

// Maintain an alive state of an audit until the audit completes
// Automatically retry until if errors of the auditing process happen
// Return if (1) audit completes; (2) retry times exceed the maximum retry times
// Throw error if this actor gets cancelled
ACTOR Future<Void> auditStorageCore(Reference<DataDistributorData> self,
                                    UID auditID,
                                    AuditType auditType,
                                    int currentRetryCount) {
	ASSERT(auditID.isValid());
	state std::shared_ptr<DDAudit> audit = getAuditFromAuditMap(self, auditType, auditID);

	state MoveKeyLockInfo lockInfo;
	lockInfo.myOwner = self->lock.myOwner;
	lockInfo.prevOwner = self->lock.prevOwner;
	lockInfo.prevWrite = self->lock.prevWrite;

	try {
		ASSERT(audit != nullptr);
		ASSERT(audit->coreState.ddId == self->ddId);
		loadAndDispatchAudit(self, audit);
		TraceEvent(SevInfo, "DDAuditStorageCoreScheduled", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditType", audit->coreState.getType())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		wait(audit->actors.getResult()); // goto exception handler if any actor is failed
		TraceEvent(SevInfo, "DDAuditStorageCoreAllActorsComplete", self->ddId)
		    .detail("AuditID", audit->coreState.id)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditType", audit->coreState.getType())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount)
		    .detail("DDDoAuditTasksIssued", audit->overallIssuedDoAuditCount)
		    .detail("DDDoAuditTasksComplete", audit->overallCompleteDoAuditCount)
		    .detail("DDDoAuditTasksSkipped", audit->overallSkippedDoAuditCount);

		if (audit->foundError) {
			audit->coreState.setPhase(AuditPhase::Error);
		} else if (audit->auditStorageAnyChildFailed) {
			audit->auditStorageAnyChildFailed = false;
			TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
			    .detail("Reason", "AuditStorageAnyChildFailed")
			    .detail("AuditID", auditID)
			    .detail("RetryCount", audit->retryCount)
			    .detail("AuditType", auditType);
			throw retry();
		} else {
			// Check audit persist progress to double check if any range omitted to be check
			if (audit->coreState.getType() == AuditType::ValidateHA ||
			    audit->coreState.getType() == AuditType::ValidateReplica) {
				bool allFinish = wait(checkAuditProgressCompleteByRange(
				    self->cx, audit->coreState.getType(), audit->coreState.id, audit->coreState.range));
				if (!allFinish) {
					TraceEvent(SevInfo, "DDAuditStorageCoreRetry", self->ddId)
					    .detail("Reason", "AuditReplicaNotFinish")
					    .detail("AuditID", auditID)
					    .detail("RetryCount", audit->retryCount)
					    .detail("AuditType", auditType);
					throw retry();
				}
			} else {
				throw not_implemented();
			}
			audit->coreState.setPhase(AuditPhase::Complete);
		}
		TraceEvent(SevVerbose, "DDAuditStorageCoreCompleteAudit", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditState", audit->coreState.toString())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		wait(persistAuditState(
		    self->cx, audit->coreState, "AuditStorageCore", lockInfo, self->ddEnabledState.isDDEnabled()));
		TraceEvent(SevVerbose, "DDAuditStorageCoreSetResult", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditState", audit->coreState.toString())
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
		removeAuditFromAuditMap(self, audit->coreState.getType(),
		                        audit->coreState.id); // remove audit

		TraceEvent(SevInfo, "DDAuditStorageCoreEnd", self->ddId)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", auditID)
		    .detail("AuditType", auditType)
		    .detail("Range", audit->coreState.range)
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			// If this audit is cancelled, the place where cancelling
			// this audit does removeAuditFromAuditMap
			throw e;
		}
		TraceEvent(SevInfo, "DDAuditStorageCoreError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("Context", audit->getDDAuditContext())
		    .detail("AuditID", auditID)
		    .detail("AuditStorageCoreGeneration", currentRetryCount)
		    .detail("RetryCount", audit->retryCount)
		    .detail("AuditType", auditType)
		    .detail("Range", audit->coreState.range);
		if (e.code() == error_code_movekeys_conflict) {
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
			// Silently exit
		} else if (e.code() == error_code_audit_storage_task_outdated) {
			// Silently exit
		} else if (e.code() == error_code_audit_storage_cancelled) {
			// If this audit is cancelled, the place where cancelling
			// this audit does removeAuditFromAuditMap
		} else if (audit->retryCount < SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX && e.code() != error_code_not_implemented) {
			audit->retryCount++;
			audit->actors.clear(true);
			TraceEvent(SevVerbose, "DDAuditStorageCoreRetry", self->ddId)
			    .detail("AuditID", auditID)
			    .detail("AuditType", auditType)
			    .detail("AuditStorageCoreGeneration", currentRetryCount)
			    .detail("RetryCount", audit->retryCount)
			    .detail("AuditExist", auditExistInAuditMap(self, auditType, auditID));
			wait(delay(0.1));
			TraceEvent(SevVerbose, "DDAuditStorageCoreRetryAfterWait", self->ddId)
			    .detail("AuditID", auditID)
			    .detail("AuditType", auditType)
			    .detail("AuditStorageCoreGeneration", currentRetryCount)
			    .detail("RetryCount", audit->retryCount)
			    .detail("AuditExist", auditExistInAuditMap(self, auditType, auditID));
			// Erase the old audit from map and spawn a new audit inherit from the old audit
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
			if (audit->coreState.getType() == AuditType::ValidateStorageServerShard) {
				runAuditStorage(self,
				                audit->coreState,
				                audit->retryCount,
				                DDAuditContext::RETRY,
				                audit->serversFinishedSSShardAudit);
			} else {
				runAuditStorage(self, audit->coreState, audit->retryCount, DDAuditContext::RETRY);
			}
		} else {
			try {
				audit->coreState.setPhase(AuditPhase::Failed);
				wait(persistAuditState(
				    self->cx, audit->coreState, "AuditStorageCoreError", lockInfo, self->ddEnabledState.isDDEnabled()));
				TraceEvent(SevWarn, "DDAuditStorageCoreSetAuditFailed", self->ddId)
				    .detail("Context", audit->getDDAuditContext())
				    .detail("AuditID", auditID)
				    .detail("AuditType", auditType)
				    .detail("AuditStorageCoreGeneration", currentRetryCount)
				    .detail("RetryCount", audit->retryCount)
				    .detail("AuditState", audit->coreState.toString());
			} catch (Error& e) {
				TraceEvent(SevWarn, "DDAuditStorageCoreErrorWhenSetAuditFailed", self->ddId)
				    .errorUnsuppressed(e)
				    .detail("Context", audit->getDDAuditContext())
				    .detail("AuditID", auditID)
				    .detail("AuditType", auditType)
				    .detail("AuditStorageCoreGeneration", currentRetryCount)
				    .detail("RetryCount", audit->retryCount)
				    .detail("AuditState", audit->coreState.toString());
				// unexpected error when persistAuditState
				// However, we do not want any audit error kills the DD
				// So, we silently remove audit from auditMap
				// As a result, this audit can be in RUNNING state on disk but not alive
				// We call this audit a zombie audit
				// Note that a client may wait for the state on disk to proceed to "complete"
				// However, this progress can never happen to a zombie audit
				// For this case, the client should be able to be timed out
				// A zombie aduit will be either: (1) resumed by the next DD; (2) removed by client
			}
			removeAuditFromAuditMap(self, audit->coreState.getType(),
			                        audit->coreState.id); // remove audit
		}
	}
	return Void();
}

// runAuditStorage is the only entry to start an Audit entity
// Three scenarios when using runAuditStorage:
// (1) When DD receives an Audit request;
// (2) When DD restarts and resume an Audit;
// (3) When an Audit gets failed and retries.
// runAuditStorage is a non-flow function which starts an audit for auditState
// with four steps (the four steps are atomic):
// (1) Validate input auditState; (2) Create audit data structure based on input auditState;
// (3) register it to dd->audits, (4) run auditStorageCore
void runAuditStorage(Reference<DataDistributorData> self,
                     AuditStorageState auditState,
                     int retryCount,
                     DDAuditContext context,
                     Optional<std::unordered_set<UID>> serversFinishedSSShardAudit) {
	// Validate input auditState
	if (auditState.getType() != AuditType::ValidateReplica) {
		throw not_implemented();
	}
	TraceEvent(SevDebug, "DDRunAuditStorage", self->ddId)
	    .detail("AuditState", auditState.toString())
	    .detail("Context", context);
	ASSERT(auditState.id.isValid());
	ASSERT(!auditState.range.empty());
	ASSERT(auditState.getPhase() == AuditPhase::Running);
	auditState.ddId = self->ddId; // make sure any existing audit state claims the current DD
	std::shared_ptr<DDAudit> audit = std::make_shared<DDAudit>(auditState);
	audit->retryCount = retryCount;
	audit->setDDAuditContext(context);
	if (serversFinishedSSShardAudit.present()) {
		audit->serversFinishedSSShardAudit = serversFinishedSSShardAudit.get();
	}
	addAuditToAuditMap(self, audit);
	audit->setAuditRunActor(auditStorageCore(self, audit->coreState.id, audit->coreState.getType(), audit->retryCount));
	return;
}

// Get audit for auditRange and auditType, if not exist, launch a new one
ACTOR Future<UID> launchAudit(Reference<DataDistributorData> self,
                              KeyRange auditRange,
                              AuditType auditType,
                              KeyValueStoreType auditStorageEngineType) {
	state MoveKeyLockInfo lockInfo;
	lockInfo.myOwner = self->lock.myOwner;
	lockInfo.prevOwner = self->lock.prevOwner;
	lockInfo.prevWrite = self->lock.prevWrite;

	state UID auditID;
	try {
		TraceEvent(SevInfo, "DDAuditStorageLaunchStarts", self->ddId)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", auditStorageEngineType)
		    .detail("RequestedRange", auditRange);
		wait(self->auditStorageInitialized.getFuture());
		// Start an audit if no audit exists
		// If existing an audit for a different purpose, send error to client
		// aka, we only allow one audit at a time for all purposes
		if (existAuditInAuditMapForType(self, auditType)) {
			std::shared_ptr<DDAudit> audit;
			// find existing audit with requested type and range
			for (auto& [id, currentAudit] : getAuditsForType(self, auditType)) {
				TraceEvent(SevInfo, "DDAuditStorageLaunchCheckExisting", self->ddId)
				    .detail("AuditID", currentAudit->coreState.id)
				    .detail("AuditType", currentAudit->coreState.getType())
				    .detail("KeyValueStoreType", auditStorageEngineType)
				    .detail("AuditPhase", currentAudit->coreState.getPhase())
				    .detail("AuditRange", currentAudit->coreState.range)
				    .detail("AuditRetryTime", currentAudit->retryCount);
				// We do not want to distinguish audit phase here
				// An audit will be gracefully removed from the map after
				// the audit enters the complete/error/failed phase
				// If an audit gets removed from the map, we think this
				// audit finishes and a new audit can be created for the
				// same time.
				if (currentAudit->coreState.range.contains(auditRange)) {
					ASSERT(auditType == currentAudit->coreState.getType());
					auditID = currentAudit->coreState.id;
					audit = currentAudit;
					break;
				}
			}
			if (audit == nullptr) { // Only one ongoing audit is allowed at a time
				throw audit_storage_exceeded_request_limit();
			}
			TraceEvent(SevInfo, "DDAuditStorageLaunchExist", self->ddId)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("AuditID", auditID)
			    .detail("RequestedRange", auditRange)
			    .detail("ExistingState", audit->coreState.toString());
		} else {
			state AuditStorageState auditState;
			auditState.setType(auditType);
			auditState.engineType = auditStorageEngineType;
			auditState.range = auditRange;
			auditState.setPhase(AuditPhase::Running);
			auditState.ddId = self->ddId; // persist ddId to new ddAudit metadata
			TraceEvent(SevVerbose, "DDAuditStorageLaunchPersistNewAuditIDBefore", self->ddId)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("Range", auditRange);
			UID auditID_ =
			    wait(persistNewAuditState(self->cx, auditState, lockInfo, self->ddEnabledState.isDDEnabled()));
			self->addActor.send(clearAuditMetadataForType(
			    self->cx, auditState.getType(), auditID_, SERVER_KNOBS->PERSIST_FINISH_AUDIT_COUNT));
			// data distribution could restart in the middle of persistNewAuditState
			// It is possible that the auditState has been written to disk before data distribution restarts,
			// hence a new audit resumption loads audits from disk and launch the audits
			// Since the resumed audit has already taken over the launchAudit job,
			// we simply retry this launchAudit, then return the audit id to client
			if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
				TraceEvent(SevDebug, "DDAuditStorageLaunchInjectActorCancelWhenPersist", self->ddId)
				    .detail("AuditID", auditID_)
				    .detail("AuditType", auditType)
				    .detail("KeyValueStoreType", auditStorageEngineType)
				    .detail("Range", auditRange);
				throw operation_failed(); // Trigger DD restart and check if resume audit is correct
			}
			TraceEvent(SevInfo, "DDAuditStorageLaunchPersistNewAuditID", self->ddId)
			    .detail("AuditID", auditID_)
			    .detail("AuditType", auditType)
			    .detail("KeyValueStoreType", auditStorageEngineType)
			    .detail("Range", auditRange);
			auditState.id = auditID_;
			auditID = auditID_;
			if (auditExistInAuditMap(self, auditType, auditID)) {
				// It is possible that the current DD is running this audit
				// Suppose DDinit re-runs right after a new audit is persisted
				// For this case, auditResume sees the new audit and resumes it
				// At this point, the new audit is already in the audit map
				return auditID;
			}
			runAuditStorage(self, auditState, 0, DDAuditContext::LAUNCH);
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "DDAuditStorageLaunchError", self->ddId)
		    .errorUnsuppressed(e)
		    .detail("AuditType", auditType)
		    .detail("KeyValueStoreType", auditStorageEngineType)
		    .detail("Range", auditRange);
		throw e;
	}
	return auditID;
}

ACTOR Future<Void> ddSplitRange(DistributorSplitRangeRequest req,
                                PromiseStream<DistributorSplitRangeRequest> manualShardSplit,
                                UID ddId) {
	try {
		TraceEvent(SevInfo, "ManualShardSplitDDReceived", ddId).detail("InputSplitPoints", req.splitPoints);
		SplitShardReply res = wait(manualShardSplit.getReply(DistributorSplitRangeRequest(req.splitPoints)));
		req.reply.send(res);
		TraceEvent(SevInfo, "ManualShardSplitDDTriggered", ddId).detail("InputSplitPoints", req.splitPoints);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "ManualShardSplitDDFailedToTrigger", ddId)
		    .errorUnsuppressed(e)
		    .detail("Reason", "DDSplitRangeError")
		    .detail("InputSplitPoints", req.splitPoints);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> dataDistributor(DataDistributorInterface di, Reference<AsyncVar<ServerDBInfo> const> db) {
	state Reference<DataDistributorData> self(new DataDistributorData(db, di.id()));
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	state PromiseStream<GetMetricsListRequest> getShardMetricsList;
	state PromiseStream<DistributorSplitRangeRequest> manualShardSplit;
	state ActorCollection actors(false);
	self->cx = openDBOnServer(db, TaskPriority::DefaultDelay, LockAware::True);
	self->addActor.send(actors.getResult());
	self->addActor.send(traceRole(Role::DATA_DISTRIBUTOR, di.id()));

	try {
		TraceEvent("DataDistributorRunning", di.id());
		self->addActor.send(waitFailureServer(di.waitFailure.getFuture()));
		self->addActor.send(cacheServerWatcher(&self->cx));
		state Future<Void> distributor =
		    reportErrorsExcept(dataDistribution(self, getShardMetricsList, manualShardSplit, &self->ddEnabledState),
		                       "DataDistribution",
		                       di.id(),
		                       &normalDataDistributorErrors());

		loop choose {
			when(wait(distributor || collection)) {
				ASSERT(false);
				throw internal_error();
			}
			when(HaltDataDistributorRequest req = waitNext(di.haltDataDistributor.getFuture())) {
				req.reply.send(Void());
				TraceEvent("DataDistributorHalted", di.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(GetDataDistributorMetricsRequest req = waitNext(di.dataDistributorMetrics.getFuture())) {
				actors.add(ddGetMetrics(req, getShardMetricsList));
			}
			when(DistributorSnapRequest snapReq = waitNext(di.distributorSnapReq.getFuture())) {
				actors.add(ddSnapCreate(snapReq, db, &self->ddEnabledState));
			}
			when(DistributorExclusionSafetyCheckRequest exclCheckReq =
			         waitNext(di.distributorExclCheckReq.getFuture())) {
				actors.add(ddExclusionSafetyCheck(exclCheckReq, self, self->cx));
			}
			when(TriggerAuditRequest req = waitNext(di.triggerAudit.getFuture())) {
				if (req.cancel) {
					ASSERT(req.id.isValid());
					// actors.add(cancelAuditStorage(self, req));
					continue;
				}
				actors.add(auditStorage(self, req));
			}
			when(DistributorSplitRangeRequest splitRangeReq = waitNext(di.distributorSplitRange.getFuture())) {
				if (self->initialized) {
					actors.add(ddSplitRange(splitRangeReq, manualShardSplit, di.id()));
				} else {
					splitRangeReq.reply.sendError(manual_shard_split_failed());
					TraceEvent(SevWarn, "ManualShardSplitDDFailedToTrigger", di.id())
					    .detail("Reason", "DDNotInitialized")
					    .detail("InputSplitPoints", splitRangeReq.splitPoints);
				}
			}
		}
	} catch (Error& err) {
		if (normalDataDistributorErrors().count(err.code()) == 0) {
			TraceEvent("DataDistributorError", di.id()).errorUnsuppressed(err);
			throw err;
		}
		TraceEvent("DataDistributorDied", di.id()).errorUnsuppressed(err);
	}

	return Void();
}

static Future<ErrorOr<Void>> goodTestFuture(double duration) {
	return tag(delay(duration), ErrorOr<Void>(Void()));
}

static Future<ErrorOr<Void>> badTestFuture(double duration, Error e) {
	return tag(delay(duration), ErrorOr<Void>(e));
}

TEST_CASE("/DataDistribution/WaitForMost") {
	state std::vector<Future<ErrorOr<Void>>> futures;
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 1, operation_failed(), 0.0)); // Don't wait for slowest future
		ASSERT(!futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 0, operation_failed(), 0.0)); // Wait for all futures
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 1, operation_failed(), 1.0)); // Wait for slowest future
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		wait(waitForMost(futures, 1, operation_failed(), 1.0)); // Error ignored
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		try {
			wait(waitForMost(futures, 0, operation_failed(), 1.0));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	return Void();
}
