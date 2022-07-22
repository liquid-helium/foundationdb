/*
 * EncryptKeyProxyInterface.h
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

#ifndef FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#define FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#pragma once

#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/IRandom.h"
#include "flow/network.h"

#include <limits>

struct EncryptKeyProxyInterface {
	constexpr static FileIdentifier file_identifier = 1303419;
	struct LocalityData locality;
	UID myId;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltEncryptKeyProxyRequest> haltEncryptKeyProxy;
	RequestStream<struct EKPGetBaseCipherKeysByIdsRequest> getBaseCipherKeysByIds;
	RequestStream<struct EKPGetLatestBaseCipherKeysRequest> getLatestBaseCipherKeys;
	RequestStream<struct EKPGetLatestBlobMetadataRequest> getLatestBlobMetadata;

	EncryptKeyProxyInterface() {}
	explicit EncryptKeyProxyInterface(const struct LocalityData& loc, UID id) : locality(loc), myId(id) {}

	NetworkAddress address() const { return haltEncryptKeyProxy.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return haltEncryptKeyProxy.getEndpoint().addresses; }

	UID id() const { return myId; }

	bool operator==(const EncryptKeyProxyInterface& toCompare) const { return myId == toCompare.myId; }
	bool operator!=(const EncryptKeyProxyInterface& toCompare) const { return !(*this == toCompare); }

	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, locality, myId, waitFailure);
		if (Archive::isDeserializing) {
			haltEncryptKeyProxy =
			    RequestStream<struct HaltEncryptKeyProxyRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			getBaseCipherKeysByIds = RequestStream<struct EKPGetBaseCipherKeysByIdsRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(2));
			getLatestBaseCipherKeys = RequestStream<struct EKPGetLatestBaseCipherKeysRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(3));
			getLatestBlobMetadata =
			    RequestStream<struct EKPGetLatestBlobMetadataRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(4));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(haltEncryptKeyProxy.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		streams.push_back(getBaseCipherKeysByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(getLatestBaseCipherKeys.getReceiver(TaskPriority::Worker));
		streams.push_back(getLatestBlobMetadata.getReceiver(TaskPriority::Worker));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct HaltEncryptKeyProxyRequest {
	constexpr static FileIdentifier file_identifier = 2378138;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltEncryptKeyProxyRequest() : requesterID(deterministicRandom()->randomUniqueID()) {}
	explicit HaltEncryptKeyProxyRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

struct EKPBaseCipherDetails {
	constexpr static FileIdentifier file_identifier = 2149615;
	int64_t encryptDomainId;
	uint64_t baseCipherId;
	StringRef baseCipherKey;
	int64_t refreshAt;
	int64_t expireAt;

	EKPBaseCipherDetails()
	  : encryptDomainId(0), baseCipherId(0), baseCipherKey(StringRef()), refreshAt(0), expireAt(-1) {}
	explicit EKPBaseCipherDetails(int64_t dId, uint64_t id, StringRef key, Arena& arena)
	  : encryptDomainId(dId), baseCipherId(id), baseCipherKey(StringRef(arena, key)),
	    refreshAt(std::numeric_limits<int64_t>::max()), expireAt(std::numeric_limits<int64_t>::max()) {}
	explicit EKPBaseCipherDetails(int64_t dId, uint64_t id, StringRef key, Arena& arena, int64_t refAt, int64_t expAt)
	  : encryptDomainId(dId), baseCipherId(id), baseCipherKey(StringRef(arena, key)), refreshAt(refAt),
	    expireAt(expAt) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, baseCipherKey, refreshAt, expireAt);
	}
};

struct EKPGetBaseCipherKeysByIdsReply {
	constexpr static FileIdentifier file_identifier = 9485259;
	Arena arena;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetBaseCipherKeysByIdsReply() : numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, baseCipherDetails, numHits, error);
	}
};

struct EKPGetBaseCipherKeysRequestInfo {
	constexpr static FileIdentifier file_identifier = 2180516;
	// Encryption cipher domain identifier
	EncryptCipherDomainId domainId;
	// Encryption cipher KMS assigned identifier
	EncryptCipherBaseKeyId baseCipherId;
	// Encryption domain name - ancillairy metadata information, an encryption key should be uniquely identified by
	// {domainId, cipherBaseId} tuple
	EncryptCipherDomainName domainName;

	EKPGetBaseCipherKeysRequestInfo()
	  : domainId(ENCRYPT_INVALID_DOMAIN_ID), baseCipherId(ENCRYPT_INVALID_CIPHER_KEY_ID) {}
	EKPGetBaseCipherKeysRequestInfo(const EncryptCipherDomainId dId,
	                                const EncryptCipherBaseKeyId bCId,
	                                StringRef name,
	                                Arena& arena)
	  : domainId(dId), baseCipherId(bCId), domainName(StringRef(arena, name)) {}

	bool operator==(const EKPGetBaseCipherKeysRequestInfo& info) const {
		return domainId == info.domainId && baseCipherId == info.baseCipherId &&
		       (domainName.compare(info.domainName) == 0);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, baseCipherId, domainName);
	}
};

struct EKPGetBaseCipherKeysByIdsRequest {
	constexpr static FileIdentifier file_identifier = 4930263;
	Arena arena;
	std::vector<EKPGetBaseCipherKeysRequestInfo> baseCipherInfos;
	Optional<UID> debugId;
	ReplyPromise<EKPGetBaseCipherKeysByIdsReply> reply;

	EKPGetBaseCipherKeysByIdsRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, baseCipherInfos, debugId, reply);
	}
};

struct EKPGetLatestBaseCipherKeysReply {
	constexpr static FileIdentifier file_identifier = 4831583;
	Arena arena;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetLatestBaseCipherKeysReply() : numHits(0) {}
	explicit EKPGetLatestBaseCipherKeysReply(const std::vector<EKPBaseCipherDetails>& cipherDetails)
	  : baseCipherDetails(cipherDetails), numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, baseCipherDetails, numHits, error);
	}
};

struct EKPGetLatestCipherKeysRequestInfo {
	constexpr static FileIdentifier file_identifier = 2180516;
	// Encryption domain identifier
	EncryptCipherDomainId domainId;
	// Encryption domain name - ancillairy metadata information, an encryption key should be uniquely identified by
	// {domainId, cipherBaseId} tuple
	EncryptCipherDomainName domainName;

	EKPGetLatestCipherKeysRequestInfo() : domainId(ENCRYPT_INVALID_DOMAIN_ID) {}
	EKPGetLatestCipherKeysRequestInfo(const EncryptCipherDomainId dId, StringRef name, Arena& arena)
	  : domainId(dId), domainName(StringRef(arena, name)) {}

	bool operator==(const EKPGetLatestCipherKeysRequestInfo& info) const {
		return domainId == info.domainId && (domainName.compare(info.domainName) == 0);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, domainName);
	}
};

struct EKPGetBaseCipherKeysRequestInfo_Hash {
	std::size_t operator()(const EKPGetBaseCipherKeysRequestInfo& info) const {
		boost::hash<std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>> hasher;
		return hasher(std::make_pair(info.domainId, info.baseCipherId));
	}
};

struct EKPGetLatestBaseCipherKeysRequest {
	constexpr static FileIdentifier file_identifier = 1910123;
	Arena arena;
	std::vector<EKPGetLatestCipherKeysRequestInfo> encryptDomainInfos;
	Optional<UID> debugId;
	ReplyPromise<EKPGetLatestBaseCipherKeysReply> reply;

	EKPGetLatestBaseCipherKeysRequest() {}
	explicit EKPGetLatestBaseCipherKeysRequest(const std::vector<EKPGetLatestCipherKeysRequestInfo>& infos)
	  : encryptDomainInfos(infos) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, encryptDomainInfos, debugId, reply);
	}
};

// partition and credentials information for a given blob domain

struct EKPGetLatestBlobMetadataReply {
	constexpr static FileIdentifier file_identifier = 5761581;
	Standalone<VectorRef<BlobMetadataDetailsRef>> blobMetadataDetails;

	EKPGetLatestBlobMetadataReply() {}
	explicit EKPGetLatestBlobMetadataReply(const Standalone<VectorRef<BlobMetadataDetailsRef>>& blobMetadataDetails)
	  : blobMetadataDetails(blobMetadataDetails) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, blobMetadataDetails);
	}
};

struct EKPGetLatestBlobMetadataRequest {
	constexpr static FileIdentifier file_identifier = 3821549;
	std::vector<BlobMetadataDomainId> domainIds;
	Optional<UID> debugId;
	ReplyPromise<EKPGetLatestBlobMetadataReply> reply;

	EKPGetLatestBlobMetadataRequest() {}
	explicit EKPGetLatestBlobMetadataRequest(const std::vector<BlobMetadataDomainId>& ids) : domainIds(ids) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainIds, debugId, reply);
	}
};

#endif