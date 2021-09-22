/*
 *DataLossRecovery.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <cstdint>
#include <limits>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const Optional<Value>& value) {
	return value.present() ? value.get().toString() : "Value Not Found.";
}
} // namespace

struct DataLossRecoveryWorkload : TestWorkload {
	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	const bool enabled;
	bool pass;
	NetworkAddress addr;

	DataLossRecoveryWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), startMoveKeysParallelismLock(1), finishMoveKeysParallelismLock(1), enabled(!clientId),
	    pass(true) {}

	void validationFailed(Optional<Value>& expectedValue, Optional<Value>& actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "DataLossRecovery"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		std::cout << clientId << " started" << std::endl;
		if (!enabled) {
			std::cout << clientId << " skipping" << std::endl;
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(DataLossRecoveryWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;
		state Value newValue = "TestNewValue"_sr;

		wait(self->writeAndVerify(self, cx, key, oldValue));
		int ignore = wait(setDDMode(cx, 0));
		state NetworkAddress address = wait(self->disableDDAndMoveShard(self, cx, KeyRangeRef(key, endKey)));
		wait(self->readAndVerify(self, cx, key, oldValue));
		self->killProcess(self, address);
		wait(self->readAndVerify(self, cx, key, "Timeout"_sr));

		state Transaction tr(cx);
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(key));
		std::cout << "After kill, source server size: " << addresses.size() << "Address: " << addresses[0] << std::endl;

		int ignore = wait(setDDMode(cx, 1));
		wait(self->exclude(self, cx, key));
		wait(delay(1.0));
		wait(self->readAndVerify(self, cx, key, Optional<Value>()));
		std::cout << "Read done after excluding server." << std::endl;
		wait(self->writeAndVerify(self, cx, key, newValue));

		return Void();
	}

	ACTOR Future<Void> readAndVerify(DataLossRecoveryWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 Optional<Value> expectedValue) {
		state Transaction tr(cx);

		loop {
			tr.reset();
			try {
				state Optional<Value> res = wait(timeout(tr.get(key), 10.0, Optional<Value>("Timeout"_sr)));
				std::cout << "Read: " << printValue(res) << ", expected: " << printValue(expectedValue) << std::endl;
				if (res != expectedValue) {
					self->validationFailed(expectedValue, res);
				}
				break;
			} catch (Error& e) {
				std::cout << "Read error: " << e.name() << std::endl;
				wait(tr.onError(e));
			}
		}

		std::cout << "Exp: " << printValue(expectedValue) << std::endl;

		return Void();
	}

	ACTOR Future<Void> writeAndVerify(DataLossRecoveryWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);

		loop {
			tr.reset();
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeout(tr.commit(), 10.0, Void()));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		std::cout << "Write Done" << std::endl;

		wait(self->readAndVerify(self, cx, key, value));

		return Void();
	}

	ACTOR Future<Void> exclude(DataLossRecoveryWorkload* self, Database cx, Key key) {
		state Transaction tr(cx);
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(key));
		std::cout << "Excluding: source server size: " << addresses.size() << "Address: " << addresses[0] << std::endl;
		state std::vector<AddressExclusion> servers;
		servers.push_back(AddressExclusion(self->addr.ip, self->addr.port));
		std::cout << "Excluding " << self->addr.toString() << std::endl;
		loop {
			tr.reset();
			try {
				excludeServers(tr, servers, true);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		std::cout << "Waiting for exclude to complete..." << std::endl;
		// Wait until all data are moved out of servers.
		std::set<NetworkAddress> inProgress = wait(checkForExcludingServers(cx, servers, true));
		ASSERT(inProgress.empty());

		std::cout << "Exclude done." << std::endl;
		return Void();
	}

	// Move keys to a random selected team consisting of a single SS, after disabling DD, so that keys won't be
	// kept in the new team until DD is enabled.
	// Returns the address of the single SS of the new team.
	ACTOR Future<NetworkAddress> disableDDAndMoveShard(DataLossRecoveryWorkload* self, Database cx, KeyRange keys) {
		// Disable DD to avoid DD undoing of our move.
		// state int oldMode = wait(setDDMode(cx, 0));
		state NetworkAddress addr;

		// Pick a random SS as the dest, keys will reside on a single server after the move.
		state std::vector<UID> dest;
		while (dest.empty()) {
			std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
			if (!interfs.empty()) {
				const auto& interf = interfs[random() % interfs.size()];
				if (g_simulator.protectedAddresses.count(interf.address()) == 0) {
					dest.push_back(interf.uniqueID);
					self->addr = interf.address();
					addr = interf.address();
				}
			}
		}

		state DDEnabledState ddEnabledState;
		ddEnabledState.setSkipCheckMoveKeysLock(true);
		wait(moveKeys(cx,
		              keys,
		              dest,
		              dest,
		              MoveKeysLock(),
		              Promise<Void>(),
		              &self->startMoveKeysParallelismLock,
		              &self->finishMoveKeysParallelismLock,
		              false,
		              UID(), // for logging only
		              &ddEnabledState));

		TraceEvent("TestKeyMoved").detail("NewTeam", describe(dest)).detail("Address", addr.toString());

		state Transaction tr(cx);
		Standalone<VectorRef<const char*>> addresses = wait(tr.getAddressesForKey(keys.begin));

		// The move function is not what we are testing here, crash the test if the move fails.
		std::cout << "source size: " << addresses.size() << "Address: " << addresses[0] << std::endl;
		ASSERT(addresses.size() == 1);
		ASSERT(std::string(addresses[0]) == addr.toString());

		// tr.reset();
		// Standalone<VectorRef<const char*>> endKeyAddrs = wait(tr.getAddressesForKey(keys.end));

		// The move function is not what we are testing here, crash the test if the move fails.
		// std::cout << "source size for endKey: " << endKeyAddrs.size() << std::endl;

		// std::cout << "Moved to " << self->addr.toString() << std::endl;

		// int ignore = wait(setDDMode(cx, oldMode));

		return addr;
	}

	void killProcess(DataLossRecoveryWorkload* self, const NetworkAddress& addr) {
		ISimulator::ProcessInfo* process = g_simulator.getProcessByAddress(addr);
		ASSERT(process->address == addr);
		g_simulator.killProcess(process, ISimulator::KillInstantly);
		TraceEvent("TestTeamKilled").detail("Address", addr.toString());
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DataLossRecoveryWorkload> DataLossRecoveryWorkloadFactory("DataLossRecovery");