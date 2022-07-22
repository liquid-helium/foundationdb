/*
 * BlobWorkerInterface.h
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

#ifndef FDBCLIENT_BLOBWORKERINTERFACE_H
#define FDBCLIENT_BLOBWORKERINTERFACE_H
#pragma once

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/StorageServerInterface.h" // for TenantInfo - should we refactor that elsewhere?

struct BlobWorkerInterface {
	constexpr static FileIdentifier file_identifier = 8358753;
	// TODO: mimic what StorageServerInterface does with sequential endpoint IDs
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct BlobGranuleFileRequest> blobGranuleFileRequest;
	RequestStream<struct AssignBlobRangeRequest> assignBlobRangeRequest;
	RequestStream<struct RevokeBlobRangeRequest> revokeBlobRangeRequest;
	RequestStream<struct GetGranuleAssignmentsRequest> granuleAssignmentsRequest;
	RequestStream<struct GranuleStatusStreamRequest> granuleStatusStreamRequest;
	RequestStream<struct HaltBlobWorkerRequest> haltBlobWorker;
	RequestStream<struct FlushGranuleRequest> flushGranuleRequest;

	struct LocalityData locality;
	UID myId;

	BlobWorkerInterface() {}
	explicit BlobWorkerInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {
		// TODO: specify endpoint priorities?
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(blobGranuleFileRequest.getReceiver());
		streams.push_back(assignBlobRangeRequest.getReceiver());
		streams.push_back(revokeBlobRangeRequest.getReceiver());
		streams.push_back(granuleAssignmentsRequest.getReceiver());
		streams.push_back(granuleStatusStreamRequest.getReceiver());
		streams.push_back(haltBlobWorker.getReceiver());
		streams.push_back(flushGranuleRequest.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
	UID id() const { return myId; }
	NetworkAddress address() const { return blobGranuleFileRequest.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return blobGranuleFileRequest.getEndpoint().getStableAddress(); }
	bool operator==(const BlobWorkerInterface& r) const { return id() == r.id(); }
	bool operator!=(const BlobWorkerInterface& r) const { return !(*this == r); }
	std::string toString() const { return id().shortString(); }

	template <class Archive>
	void serialize(Archive& ar) {
		// use adjusted endpoints
		serializer(ar, myId, locality, waitFailure);
		if (Archive::isDeserializing) {
			blobGranuleFileRequest =
			    RequestStream<struct BlobGranuleFileRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			assignBlobRangeRequest =
			    RequestStream<struct AssignBlobRangeRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(2));
			revokeBlobRangeRequest =
			    RequestStream<struct RevokeBlobRangeRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(3));
			granuleAssignmentsRequest =
			    RequestStream<struct GetGranuleAssignmentsRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(4));
			granuleStatusStreamRequest =
			    RequestStream<struct GranuleStatusStreamRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(5));
			haltBlobWorker =
			    RequestStream<struct HaltBlobWorkerRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(6));
			flushGranuleRequest =
			    RequestStream<struct FlushGranuleRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(7));
		}
	}
};

struct BlobGranuleFileReply {
	constexpr static FileIdentifier file_identifier = 6858612;
	Arena arena;
	VectorRef<BlobGranuleChunkRef> chunks;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, chunks, arena);
	}
};

// TODO could do a reply promise stream of file mutations to bound memory requirements?
// Have to load whole snapshot file into memory though so it doesn't actually matter too much
struct BlobGranuleFileRequest {
	constexpr static FileIdentifier file_identifier = 4150141;
	Arena arena;
	KeyRangeRef keyRange;
	Version beginVersion = 0;
	Version readVersion;
	bool canCollapseBegin = true;
	TenantInfo tenantInfo;
	ReplyPromise<BlobGranuleFileReply> reply;

	BlobGranuleFileRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, beginVersion, readVersion, canCollapseBegin, tenantInfo, reply, arena);
	}
};

struct RevokeBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 4844288;
	Arena arena;
	KeyRangeRef keyRange;
	int64_t managerEpoch;
	int64_t managerSeqno;
	bool dispose;
	ReplyPromise<Void> reply;

	RevokeBlobRangeRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, managerEpoch, managerSeqno, dispose, reply, arena);
	}
};

/*
 * Continue: Blob worker should continue handling a granule that was evaluated for a split
 * Normal: Blob worker should open the granule and start processing it
 */
enum AssignRequestType { Normal = 0, Continue = 1 };

struct AssignBlobRangeRequest {
	constexpr static FileIdentifier file_identifier = 905381;
	Arena arena;
	KeyRangeRef keyRange;
	int64_t managerEpoch;
	int64_t managerSeqno;
	// If continueAssignment is true, this is just to instruct the worker that it *still* owns the range, so it should
	// re-snapshot it and continue.

	AssignRequestType type;

	ReplyPromise<Void> reply;

	AssignBlobRangeRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, managerEpoch, managerSeqno, type, reply, arena);
	}
};

// reply per granule
// TODO: could eventually add other types of metrics to report back to the manager here
struct GranuleStatusReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 7563104;

	KeyRange granuleRange;
	bool doSplit;
	bool writeHotSplit;
	int64_t continueEpoch;
	int64_t continueSeqno;
	UID granuleID;
	Version startVersion;
	Version blockedVersion;
	bool mergeCandidate;
	int64_t originalEpoch;
	int64_t originalSeqno;

	GranuleStatusReply() {}
	explicit GranuleStatusReply(KeyRange range,
	                            bool doSplit,
	                            bool writeHotSplit,
	                            int64_t continueEpoch,
	                            int64_t continueSeqno,
	                            UID granuleID,
	                            Version startVersion,
	                            Version blockedVersion,
	                            bool mergeCandidate,
	                            int64_t originalEpoch,
	                            int64_t originalSeqno)
	  : granuleRange(range), doSplit(doSplit), writeHotSplit(writeHotSplit), continueEpoch(continueEpoch),
	    continueSeqno(continueSeqno), granuleID(granuleID), startVersion(startVersion), blockedVersion(blockedVersion),
	    mergeCandidate(mergeCandidate), originalEpoch(originalEpoch), originalSeqno(originalSeqno) {}

	int expectedSize() const { return sizeof(GranuleStatusReply) + granuleRange.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           ReplyPromiseStreamReply::acknowledgeToken,
		           ReplyPromiseStreamReply::sequence,
		           granuleRange,
		           doSplit,
		           writeHotSplit,
		           continueEpoch,
		           continueSeqno,
		           granuleID,
		           startVersion,
		           blockedVersion,
		           mergeCandidate,
		           originalEpoch,
		           originalSeqno);
	}
};

// manager makes one request per worker, it sends all range updates through this stream
struct GranuleStatusStreamRequest {
	constexpr static FileIdentifier file_identifier = 2289677;

	int64_t managerEpoch;

	ReplyPromiseStream<GranuleStatusReply> reply;

	GranuleStatusStreamRequest() {}
	explicit GranuleStatusStreamRequest(int64_t managerEpoch) : managerEpoch(managerEpoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, reply);
	}
};

struct HaltBlobWorkerRequest {
	constexpr static FileIdentifier file_identifier = 1985879;
	UID requesterID;
	ReplyPromise<Void> reply;

	int64_t managerEpoch;

	HaltBlobWorkerRequest() {}
	explicit HaltBlobWorkerRequest(int64_t managerEpoch, UID uid) : requesterID(uid), managerEpoch(managerEpoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, requesterID, reply);
	}
};

struct GranuleAssignmentRef {
	KeyRangeRef range;
	int64_t epochAssigned;
	int64_t seqnoAssigned;

	GranuleAssignmentRef() {}

	explicit GranuleAssignmentRef(KeyRangeRef range, int64_t epochAssigned, int64_t seqnoAssigned)
	  : range(range), epochAssigned(epochAssigned), seqnoAssigned(seqnoAssigned) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, epochAssigned, seqnoAssigned);
	}
};

struct GetGranuleAssignmentsReply {
	constexpr static FileIdentifier file_identifier = 9191718;
	Arena arena;
	VectorRef<GranuleAssignmentRef> assignments;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, assignments, arena);
	}
};

struct GetGranuleAssignmentsRequest {
	constexpr static FileIdentifier file_identifier = 4121494;
	int64_t managerEpoch;
	ReplyPromise<GetGranuleAssignmentsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, reply);
	}
};

struct FlushGranuleRequest {
	constexpr static FileIdentifier file_identifier = 5855784;
	int64_t managerEpoch;
	KeyRange granuleRange;
	Version flushVersion;
	ReplyPromise<Void> reply;

	FlushGranuleRequest() : managerEpoch(-1), flushVersion(invalidVersion) {}
	explicit FlushGranuleRequest(int64_t managerEpoch, KeyRange granuleRange, Version flushVersion)
	  : managerEpoch(managerEpoch), granuleRange(granuleRange), flushVersion(flushVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, managerEpoch, granuleRange, flushVersion, reply);
	}
};

#endif
