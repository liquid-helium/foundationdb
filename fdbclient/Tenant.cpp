/*
 * Tenant.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "libb64/encode.h"
#include "flow/UnitTest.h"

Key TenantMapEntry::idToPrefix(int64_t id) {
	int64_t swapped = bigEndian64(id);
	return StringRef(reinterpret_cast<const uint8_t*>(&swapped), 8);
}

int64_t TenantMapEntry::prefixToId(KeyRef prefix) {
	ASSERT(prefix.size() == 8);
	int64_t id = *reinterpret_cast<const int64_t*>(prefix.begin());
	id = bigEndian64(id);
	ASSERT(id >= 0);
	return id;
}

std::string TenantMapEntry::tenantStateToString(TenantState tenantState) {
	switch (tenantState) {
	case TenantState::REGISTERING:
		return "registering";
	case TenantState::READY:
		return "ready";
	case TenantState::REMOVING:
		return "removing";
	case TenantState::UPDATING_CONFIGURATION:
		return "updating configuration";
	case TenantState::ERROR:
		return "error";
	default:
		UNREACHABLE();
	}
}

TenantState TenantMapEntry::stringToTenantState(std::string stateStr) {
	if (stateStr == "registering") {
		return TenantState::REGISTERING;
	} else if (stateStr == "ready") {
		return TenantState::READY;
	} else if (stateStr == "removing") {
		return TenantState::REMOVING;
	} else if (stateStr == "updating configuration") {
		return TenantState::UPDATING_CONFIGURATION;
	} else if (stateStr == "error") {
		return TenantState::ERROR;
	}

	UNREACHABLE();
}

TenantMapEntry::TenantMapEntry() {}
TenantMapEntry::TenantMapEntry(int64_t id, TenantState tenantState, bool encrypted)
  : tenantState(tenantState), encrypted(encrypted) {
	setId(id);
}
TenantMapEntry::TenantMapEntry(int64_t id,
                               TenantState tenantState,
                               Optional<TenantGroupName> tenantGroup,
                               bool encrypted)
  : tenantState(tenantState), tenantGroup(tenantGroup), encrypted(encrypted) {
	setId(id);
}

void TenantMapEntry::setId(int64_t id) {
	ASSERT(id >= 0);
	this->id = id;
	prefix = idToPrefix(id);
}

std::string TenantMapEntry::toJson(int apiVersion) const {
	json_spirit::mObject tenantEntry;
	tenantEntry["id"] = id;
	tenantEntry["encrypted"] = encrypted;

	if (apiVersion >= 720 || apiVersion == Database::API_VERSION_LATEST) {
		json_spirit::mObject prefixObject;
		std::string encodedPrefix = base64::encoder::from_string(prefix.toString());
		// Remove trailing newline
		encodedPrefix.resize(encodedPrefix.size() - 1);

		prefixObject["base64"] = encodedPrefix;
		prefixObject["printable"] = printable(prefix);
		tenantEntry["prefix"] = prefixObject;
	} else {
		// This is not a standard encoding in JSON, and some libraries may not be able to easily decode it
		tenantEntry["prefix"] = prefix.toString();
	}

	tenantEntry["tenant_state"] = TenantMapEntry::tenantStateToString(tenantState);
	if (assignedCluster.present()) {
		tenantEntry["assigned_cluster"] = assignedCluster.get().toString();
	}
	if (tenantGroup.present()) {
		json_spirit::mObject tenantGroupObject;
		std::string encodedTenantGroup = base64::encoder::from_string(tenantGroup.get().toString());
		// Remove trailing newline
		encodedTenantGroup.resize(encodedTenantGroup.size() - 1);

		tenantGroupObject["base64"] = encodedTenantGroup;
		tenantGroupObject["printable"] = printable(tenantGroup.get());
		tenantEntry["tenant_group"] = tenantGroupObject;
	}

	return json_spirit::write_string(json_spirit::mValue(tenantEntry));
}

bool TenantMapEntry::matchesConfiguration(TenantMapEntry const& other) const {
	return tenantGroup == other.tenantGroup;
}

void TenantMapEntry::configure(Standalone<StringRef> parameter, Optional<Value> value) {
	if (parameter == "tenant_group"_sr) {
		tenantGroup = value;
	} else {
		TraceEvent(SevWarnAlways, "UnknownTenantConfigurationParameter").detail("Parameter", parameter);
		throw invalid_tenant_configuration();
	}
}

TenantMetadataSpecification& TenantMetadata::instance() {
	static TenantMetadataSpecification _instance = TenantMetadataSpecification("\xff/"_sr);
	return _instance;
}

Key TenantMetadata::tenantMapPrivatePrefix() {
	static Key _prefix = "\xff"_sr.withSuffix(tenantMap().subspace.begin);
	return _prefix;
}

TEST_CASE("/fdbclient/TenantMapEntry/Serialization") {
	TenantMapEntry entry1(1, TenantState::READY, false);
	ASSERT(entry1.prefix == "\x00\x00\x00\x00\x00\x00\x00\x01"_sr);
	TenantMapEntry entry2 = TenantMapEntry::decode(entry1.encode());
	ASSERT(entry1.id == entry2.id && entry1.prefix == entry2.prefix);

	TenantMapEntry entry3(std::numeric_limits<int64_t>::max(), TenantState::READY, false);
	ASSERT(entry3.prefix == "\x7f\xff\xff\xff\xff\xff\xff\xff"_sr);
	TenantMapEntry entry4 = TenantMapEntry::decode(entry3.encode());
	ASSERT(entry3.id == entry4.id && entry3.prefix == entry4.prefix);

	for (int i = 0; i < 100; ++i) {
		int bits = deterministicRandom()->randomInt(1, 64);
		int64_t min = bits == 1 ? 0 : (UINT64_C(1) << (bits - 1));
		int64_t maxPlusOne = std::min<uint64_t>(UINT64_C(1) << bits, std::numeric_limits<int64_t>::max());
		int64_t id = deterministicRandom()->randomInt64(min, maxPlusOne);

		TenantMapEntry entry(id, TenantState::READY, false);
		int64_t bigEndianId = bigEndian64(id);
		ASSERT(entry.id == id && entry.prefix == StringRef(reinterpret_cast<uint8_t*>(&bigEndianId), 8));

		TenantMapEntry decodedEntry = TenantMapEntry::decode(entry.encode());
		ASSERT(decodedEntry.id == entry.id && decodedEntry.prefix == entry.prefix);
	}

	return Void();
}
