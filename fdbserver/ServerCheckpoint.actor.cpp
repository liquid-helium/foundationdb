/*
 *ServerCheckpoint.actor.cpp
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

#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
	const CheckpointFormat format = checkpoint.getFormat();
	if (format == RocksDBColumnFamily || format == RocksDB) {
		return newRocksDBCheckpointReader(checkpoint, logID);
	} else {
		throw not_implemented();
	}

	return nullptr;
}

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));
	state CheckpointFormat format = checkpoint.getFormat();
	if (format == RocksDBColumnFamily || format == RocksDB) {
		wait(deleteRocksCheckpoint(checkpoint));
	} else {
		throw not_implemented();
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent("FetchCheckpointBegin", initialState.checkpointID).detail("CheckpointMetaData", initialState.toString());
	state CheckpointMetaData result;
	const CheckpointFormat format = initialState.getFormat();
	if (format == RocksDBColumnFamily || format == RocksDB) {
		CheckpointMetaData _result = wait(fetchRocksDBCheckpoint(cx, initialState, dir, cFun));
		result = _result;
	} else {
		throw not_implemented();
	}

	TraceEvent("FetchCheckpointEnd", initialState.checkpointID).detail("CheckpointMetaData", result.toString());
	return result;
}

ACTOR Future<std::vector<CheckpointMetaData>> fetchCheckpoints(
    Database cx,
    std::vector<CheckpointMetaData> initialStates,
    std::string dir,
    std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	std::vector<Future<CheckpointMetaData>> actors;
	for (const auto& checkpoint : initialStates) {
		actors.push_back(fetchCheckpoint(cx, checkpoint, dir, cFun));
	}
	std::vector<CheckpointMetaData> res = wait(getAll(actors));
	return res;
}