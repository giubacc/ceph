// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include <string>

#include "driver/sfs/sqlite/buckets/multipart_definitions.h"
#include "rgw/driver/sfs/sqlite/objects/object_definitions.h"
#include "rgw/driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "rgw_common.h"
namespace rgw::sal::sfs::sqlite {

using BLOB = std::vector<char>;

// bucket to be mapped in DB
// Optional values mean they might have (or not) a value defined.
// Blobs are stored as std::vector<char> but we could specialize the encoder and decoder templates
// from sqlite_orm to store blobs in any user defined type.
struct DBBucket {
  std::string bucket_name;
  std::string bucket_id;
  std::optional<std::string> tenant;
  std::optional<std::string> marker;
  std::optional<int> size;
  std::optional<int> size_rounded;
  std::optional<BLOB> creation_time;
  std::optional<int> count;
  std::optional<std::string> placement_name;
  std::optional<std::string> placement_storage_class;
  std::string owner_id;
  std::optional<uint32_t> flags;
  std::optional<std::string> zone_group;
  std::optional<bool> has_instance_obj;
  std::optional<BLOB> quota;
  std::optional<bool> requester_pays;
  std::optional<bool> has_website;
  std::optional<BLOB> website_conf;
  std::optional<bool> swift_versioning;
  std::optional<std::string> swift_ver_location;
  std::optional<BLOB> mdsearch_config;
  std::optional<std::string> new_bucket_instance_id;
  std::optional<BLOB> object_lock;
  std::optional<BLOB> sync_policy_info_groups;
  std::optional<BLOB> bucket_attrs;
  std::optional<int> bucket_version;
  std::optional<std::string> bucket_version_tag;
  std::optional<BLOB> mtime;
  bool deleted;
};

// Struct with information needed by SAL layer
struct DBOPBucketInfo {
  RGWBucketInfo binfo;
  Attrs battrs;
  bool deleted{false};

  DBOPBucketInfo() = default;

  DBOPBucketInfo(const RGWBucketInfo& info, const Attrs& attrs)
      : binfo(info), battrs(attrs) {}

  DBOPBucketInfo(const DBOPBucketInfo& other) = default;
  DBOPBucketInfo& operator=(const DBOPBucketInfo& other) = default;

  bool operator==(const DBOPBucketInfo& other) const {
    if (this->deleted != other.deleted) return false;
    if (this->battrs != other.battrs) return false;
    ceph::bufferlist this_binfo_bl;
    this->binfo.encode(this_binfo_bl);
    ceph::bufferlist other_binfo_bl;
    other.binfo.encode(other_binfo_bl);
    return this_binfo_bl == other_binfo_bl;
  }
};

using DBDeletedObjectItem =
    std::tuple<decltype(DBObject::uuid), decltype(DBVersionedObject::id)>;

using DBDeletedObjectItems = std::vector<DBDeletedObjectItem>;

/// DBDeletedObjectItem helpers
inline decltype(DBObject::uuid) get_uuid(const DBDeletedObjectItem& item) {
  return std::get<0>(item);
}

inline decltype(DBVersionedObject::id) get_version_id(
    const DBDeletedObjectItem& item
) {
  return std::get<1>(item);
}
}  // namespace rgw::sal::sfs::sqlite
