/*
 * TenantManagementWorkload.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/libb64/decode.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementWorkload : TestWorkload {
	struct TenantState {
		int64_t id;
		bool empty;

		TenantState() : id(-1), empty(true) {}
		TenantState(int64_t id, bool empty) : id(id), empty(empty) {}
	};

	std::map<TenantName, TenantState> createdTenants;
	int64_t maxId = -1;

	const Key keyName = "key"_sr;
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	TenantName localTenantNamePrefix;

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl<true>::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl<true>::mapSubRange.begin);

	int maxTenants;
	double testDuration;

	enum class OperationType { SPECIAL_KEYS, MANAGEMENT_DATABASE, MANAGEMENT_TRANSACTION };

	static OperationType randomOperationType() {
		int randomNum = deterministicRandom()->randomInt(0, 3);
		if (randomNum == 0) {
			return OperationType::SPECIAL_KEYS;
		} else if (randomNum == 1) {
			return OperationType::MANAGEMENT_DATABASE;
		} else {
			return OperationType::MANAGEMENT_TRANSACTION;
		}
	}

	TenantManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		testDuration = getOption(options, "testDuration"_sr, 60.0);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
	}

	std::string description() const override { return "TenantManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		if (self->clientId == 0) {
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					tr.set(self->keyName, self->noTenantValue);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	TenantName chooseTenantName(bool allowSystemTenant) {
		TenantName tenant(format(
		    "%s%08d", localTenantNamePrefix.toString().c_str(), deterministicRandom()->randomInt(0, maxTenants)));
		if (allowSystemTenant && deterministicRandom()->random01() < 0.02) {
			tenant = tenant.withPrefix("\xff"_sr);
		}

		return tenant;
	}

	ACTOR Future<Void> createTenant(Database cx, TenantManagementWorkload* self) {
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		int numTenants = 1;

		// For transaction-based operations, test creating multiple tenants in the same transaction
		if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numTenants = deterministicRandom()->randomInt(1, 5);
		}

		state bool alreadyExists = false;
		state bool hasSystemTenant = false;

		state std::set<TenantName> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			tenantsToCreate.insert(tenant);

			alreadyExists = alreadyExists || self->createdTenants.count(tenant);
			hasSystemTenant = hasSystemTenant || tenant.startsWith("\xff"_sr);
		}

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				if (operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					for (auto tenant : tenantsToCreate) {
						tr->set(self->specialKeysTenantMapPrefix.withSuffix(tenant), ""_sr);
					}
					wait(tr->commit());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					ASSERT(tenantsToCreate.size() == 1);
					wait(success(TenantAPI::createTenant(cx.getReference(), *tenantsToCreate.begin())));
				} else {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					int64_t _nextId = wait(TenantAPI::getNextTenantId(tr));
					int64_t nextId = _nextId;

					std::vector<Future<Void>> createFutures;
					for (auto tenant : tenantsToCreate) {
						createFutures.push_back(success(TenantAPI::createTenantTransaction(tr, tenant, nextId++)));
					}
					tr->set(tenantLastIdKey, TenantMapEntry::idToPrefix(nextId - 1));
					wait(waitForAll(createFutures));
					wait(tr->commit());
				}

				if (operationType == OperationType::MANAGEMENT_DATABASE) {
					ASSERT(!alreadyExists);
				}

				ASSERT(!hasSystemTenant);

				state std::set<TenantName>::iterator tenantItr;
				for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
					if (self->createdTenants.count(*tenantItr)) {
						continue;
					}

					state Optional<TenantMapEntry> entry = wait(TenantAPI::tryGetTenant(cx.getReference(), *tenantItr));
					ASSERT(entry.present());
					ASSERT(entry.get().id > self->maxId);

					self->maxId = entry.get().id;
					self->createdTenants[*tenantItr] = TenantState(entry.get().id, true);

					state bool insertData = deterministicRandom()->random01() < 0.5;
					if (insertData) {
						state Transaction insertTr(cx, *tenantItr);
						loop {
							try {
								insertTr.set(self->keyName, *tenantItr);
								wait(insertTr.commit());
								break;
							} catch (Error& e) {
								wait(insertTr.onError(e));
							}
						}

						self->createdTenants[*tenantItr].empty = false;

						state Transaction checkTr(cx);
						loop {
							try {
								checkTr.setOption(FDBTransactionOptions::RAW_ACCESS);
								Optional<Value> val = wait(checkTr.get(self->keyName.withPrefix(entry.get().prefix)));
								ASSERT(val.present());
								ASSERT(val.get() == *tenantItr);
								break;
							} catch (Error& e) {
								wait(checkTr.onError(e));
							}
						}
					}

					wait(self->checkTenant(cx, self, *tenantItr, self->createdTenants[*tenantItr]));
				}
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_invalid_tenant_name) {
					ASSERT(hasSystemTenant);
					return Void();
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_already_exists) {
						ASSERT(alreadyExists && operationType == OperationType::MANAGEMENT_DATABASE);
					} else {
						ASSERT(tenantsToCreate.size() == 1);
						TraceEvent(SevError, "CreateTenantFailure")
						    .error(e)
						    .detail("TenantName", *tenantsToCreate.begin());
					}
					return Void();
				} else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						for (auto tenant : tenantsToCreate) {
							TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
						}
						return Void();
					}
				}
			}
		}
	}

	ACTOR Future<Void> deleteTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(true);
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		state Optional<TenantName> endTenant = operationType != OperationType::MANAGEMENT_DATABASE &&
		                                               !beginTenant.startsWith("\xff"_sr) &&
		                                               deterministicRandom()->random01() < 0.2
		                                           ? Optional<TenantName>(self->chooseTenantName(false))
		                                           : Optional<TenantName>();

		if (endTenant.present() && endTenant < beginTenant) {
			TenantName temp = beginTenant;
			beginTenant = endTenant.get();
			endTenant = temp;
		}

		auto itr = self->createdTenants.find(beginTenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state bool isEmpty = true;

		state std::vector<TenantName> tenants;
		if (!endTenant.present()) {
			tenants.push_back(beginTenant);
		} else if (endTenant.present()) {
			for (auto itr = self->createdTenants.lower_bound(beginTenant);
			     itr != self->createdTenants.end() && itr->first < endTenant.get();
			     ++itr) {
				tenants.push_back(itr->first);
			}
		}

		state int tenantIndex;
		try {
			if (alreadyExists || endTenant.present()) {
				for (tenantIndex = 0; tenantIndex < tenants.size(); ++tenantIndex) {
					if (deterministicRandom()->random01() < 0.9) {
						state Transaction clearTr(cx, tenants[tenantIndex]);
						loop {
							try {
								clearTr.clear(self->keyName);
								wait(clearTr.commit());
								auto itr = self->createdTenants.find(tenants[tenantIndex]);
								ASSERT(itr != self->createdTenants.end());
								itr->second.empty = true;
								break;
							} catch (Error& e) {
								wait(clearTr.onError(e));
							}
						}
					} else {
						auto itr = self->createdTenants.find(tenants[tenantIndex]);
						ASSERT(itr != self->createdTenants.end());
						isEmpty = isEmpty && itr->second.empty;
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "DeleteTenantFailure")
			    .error(e)
			    .detail("TenantName", beginTenant)
			    .detail("EndTenant", endTenant);
			return Void();
		}

		loop {
			try {
				if (operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					Key key = self->specialKeysTenantMapPrefix.withSuffix(beginTenant);
					if (endTenant.present()) {
						tr->clear(KeyRangeRef(key, self->specialKeysTenantMapPrefix.withSuffix(endTenant.get())));
					} else {
						tr->clear(key);
					}
					wait(tr->commit());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					ASSERT(tenants.size() == 1);
					for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
						wait(TenantAPI::deleteTenant(cx.getReference(), tenants[tenantIndex]));
					}
				} else {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					std::vector<Future<Void>> deleteFutures;
					for (tenantIndex = 0; tenantIndex != tenants.size(); ++tenantIndex) {
						deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, tenants[tenantIndex]));
					}

					wait(waitForAll(deleteFutures));
					wait(tr->commit());
				}

				if (!alreadyExists && !endTenant.present() && operationType != OperationType::MANAGEMENT_DATABASE) {
					return Void();
				}

				ASSERT(alreadyExists || endTenant.present());
				ASSERT(isEmpty);
				for (auto tenant : tenants) {
					self->createdTenants.erase(tenant);
				}
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!alreadyExists && !endTenant.present());
					} else {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
					}
					return Void();
				} else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
						return Void();
					}
				}
			}
		}
	}

	ACTOR Future<Void> checkTenant(Database cx,
	                               TenantManagementWorkload* self,
	                               TenantName tenant,
	                               TenantState tenantState) {
		state Transaction tr(cx, tenant);
		loop {
			try {
				state RangeResult result = wait(tr.getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));
				if (tenantState.empty) {
					ASSERT(result.size() == 0);
				} else {
					ASSERT(result.size() == 1);
					ASSERT(result[0].key == self->keyName);
					ASSERT(result[0].value == tenant);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	static TenantMapEntry jsonToTenantMapEntry(ValueRef tenantJson) {
		json_spirit::mValue jsonObject;
		json_spirit::read_string(tenantJson.toString(), jsonObject);
		JSONDoc jsonDoc(jsonObject);

		int64_t id;
		std::string prefix;
		std::string base64Prefix;
		std::string printablePrefix;
		jsonDoc.get("id", id);
		jsonDoc.get("prefix.base64", base64Prefix);
		jsonDoc.get("prefix.printable", printablePrefix);

		prefix = base64::decoder::from_string(base64Prefix);
		ASSERT(prefix == unprintable(printablePrefix));

		Key prefixKey = KeyRef(prefix);
		TenantMapEntry entry(id);

		ASSERT(entry.prefix == prefixKey);
		return entry;
	}

	ACTOR Future<Void> getTenant(Database cx, TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end();
		state TenantState tenantState = itr->second;
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				state TenantMapEntry entry;
				if (operationType == OperationType::SPECIAL_KEYS) {
					Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
					Optional<Value> value = wait(tr->get(key));
					if (!value.present()) {
						throw tenant_not_found();
					}
					entry = TenantManagementWorkload::jsonToTenantMapEntry(value.get());
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					TenantMapEntry _entry = wait(TenantAPI::getTenant(cx.getReference(), tenant));
					entry = _entry;
				} else {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					TenantMapEntry _entry = wait(TenantAPI::getTenantTransaction(tr, tenant));
					entry = _entry;
				}
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantState.id);
				wait(self->checkTenant(cx, self, tenant, tenantState));
				return Void();
			} catch (Error& e) {
				state bool retry = true;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				} else if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantFailure").error(error).detail("TenantName", tenant);
					return Void();
				}
			}
		}
	}

	ACTOR Future<Void> listTenants(Database cx, TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->TOO_MANY, deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = TenantManagementWorkload::randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				state std::map<TenantName, TenantMapEntry> tenants;
				if (operationType == OperationType::SPECIAL_KEYS) {
					KeyRange range = KeyRangeRef(beginTenant, endTenant).withPrefix(self->specialKeysTenantMapPrefix);
					RangeResult results = wait(tr->getRange(range, limit));
					for (auto result : results) {
						tenants[result.key.removePrefix(self->specialKeysTenantMapPrefix)] =
						    TenantManagementWorkload::jsonToTenantMapEntry(result.value);
					}
				} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
					std::map<TenantName, TenantMapEntry> _tenants =
					    wait(TenantAPI::listTenants(cx.getReference(), beginTenant, endTenant, limit));
					tenants = _tenants;
				} else {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					std::map<TenantName, TenantMapEntry> _tenants =
					    wait(TenantAPI::listTenantsTransaction(tr, beginTenant, endTenant, limit));
					tenants = _tenants;
				}

				ASSERT(tenants.size() <= limit);

				auto localItr = self->createdTenants.lower_bound(beginTenant);
				auto tenantMapItr = tenants.begin();
				for (; tenantMapItr != tenants.end(); ++tenantMapItr, ++localItr) {
					ASSERT(localItr != self->createdTenants.end());
					ASSERT(localItr->first == tenantMapItr->first);
				}

				if (!(tenants.size() == limit || localItr == self->createdTenants.end())) {
					for (auto tenant : self->createdTenants) {
						TraceEvent("ExistingTenant").detail("Tenant", tenant.first);
					}
				}
				ASSERT(tenants.size() == limit || localItr == self->createdTenants.end() ||
				       localItr->first >= endTenant);
				return Void();
			} catch (Error& e) {
				state bool retry = true;
				state Error error = e;
				if (operationType != OperationType::MANAGEMENT_DATABASE) {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "ListTenantFailure")
					    .error(error)
					    .detail("BeginTenant", beginTenant)
					    .detail("EndTenant", endTenant);

					return Void();
				}
			}
		}
	}

	ACTOR Future<Void> renameTenant(Database cx, TenantManagementWorkload* self) {
		// Currently only supporting MANAGEMENT_DATABASE op, so numTenants should always be 1
		// state OperationType operationType = TenantManagementWorkload::randomOperationType();
		int numTenants = 1;

		state std::vector<TenantName> oldTenantNames;
		state std::vector<TenantName> newTenantNames;
		state bool tenantExists = false;
		state bool tenantNotFound = false;
		for (int i = 0; i < numTenants; ++i) {
			TenantName oldTenant = self->chooseTenantName(false);
			TenantName newTenant = self->chooseTenantName(false);
			newTenantNames.push_back(newTenant);
			oldTenantNames.push_back(oldTenant);
			if (!self->createdTenants.count(oldTenant)) {
				tenantNotFound = true;
			}
			if (self->createdTenants.count(newTenant)) {
				tenantExists = true;
			}
		}

		loop {
			try {
				ASSERT(oldTenantNames.size() == 1);
				state int tenantIndex = 0;
				for (; tenantIndex != oldTenantNames.size(); ++tenantIndex) {
					state TenantName oldTenantName = oldTenantNames[tenantIndex];
					state TenantName newTenantName = newTenantNames[tenantIndex];
					// Perform rename, then check against the DB for the new results
					wait(TenantAPI::renameTenant(cx.getReference(), oldTenantName, newTenantName));
					ASSERT(!tenantNotFound && !tenantExists);
					state Optional<TenantMapEntry> oldTenantEntry =
					    wait(TenantAPI::tryGetTenant(cx.getReference(), oldTenantName));
					state Optional<TenantMapEntry> newTenantEntry =
					    wait(TenantAPI::tryGetTenant(cx.getReference(), newTenantName));
					ASSERT(!oldTenantEntry.present());
					ASSERT(newTenantEntry.present());

					// Update Internal Tenant Map and check for correctness
					TenantState tState = self->createdTenants[oldTenantName];
					self->createdTenants[newTenantName] = tState;
					self->createdTenants.erase(oldTenantName);
					if (!tState.empty) {
						state Transaction insertTr(cx, newTenantName);
						loop {
							try {
								insertTr.set(self->keyName, newTenantName);
								wait(insertTr.commit());
								break;
							} catch (Error& e) {
								wait(insertTr.onError(e));
							}
						}
					}
					wait(self->checkTenant(cx, self, newTenantName, self->createdTenants[newTenantName]));
				}
				return Void();
			} catch (Error& e) {
				ASSERT(oldTenantNames.size() == 1);
				if (e.code() == error_code_tenant_not_found) {
					TraceEvent("RenameTenantOldTenantNotFound")
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
					ASSERT(tenantNotFound);
				} else if (e.code() == error_code_tenant_already_exists) {
					TraceEvent("RenameTenantNewTenantAlreadyExists")
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
					ASSERT(tenantExists);
				} else {
					TraceEvent(SevError, "RenameTenantFailure")
					    .error(e)
					    .detail("OldTenantName", oldTenantNames[0])
					    .detail("NewTenantName", newTenantNames[0]);
				}
				return Void();
			}
		}
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	ACTOR Future<Void> _start(Database cx, TenantManagementWorkload* self) {
		state double start = now();
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 5);
			if (operation == 0) {
				wait(self->createTenant(cx, self));
			} else if (operation == 1) {
				wait(self->deleteTenant(cx, self));
			} else if (operation == 2) {
				wait(self->getTenant(cx, self));
			} else if (operation == 3) {
				wait(self->listTenants(cx, self));
			} else {
				wait(self->renameTenant(cx, self));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	ACTOR Future<bool> _check(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Optional<Value> val = wait(tr.get(self->keyName));
				ASSERT(val.present() && val.get() == self->noTenantValue);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		state std::map<TenantName, TenantState>::iterator itr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			std::map<TenantName, TenantMapEntry> tenants =
			    wait(TenantAPI::listTenants(cx.getReference(), beginTenant, endTenant, 1000));

			TenantNameRef lastTenant;
			for (auto tenant : tenants) {
				ASSERT(itr != self->createdTenants.end());
				ASSERT(tenant.first == itr->first);
				checkTenants.push_back(self->checkTenant(cx, self, tenant.first, itr->second));
				lastTenant = tenant.first;
				++itr;
			}

			if (tenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(lastTenant);
			}
		}

		ASSERT(itr == self->createdTenants.end());
		wait(waitForAll(checkTenants));

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload("TenantManagement");
