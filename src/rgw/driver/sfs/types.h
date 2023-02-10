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
#ifndef RGW_STORE_SFS_TYPES_H
#define RGW_STORE_SFS_TYPES_H

#include <exception>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/ceph_mutex.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/driver/sfs/uuid_path.h"

namespace rgw::sal {
class SFStore;
}

namespace rgw::sal::sfs {

struct UnknownObjectException : public std::exception {};

struct Object {
  struct Meta {
    size_t size;
    std::string etag;
    ceph::real_time mtime;
    ceph::real_time set_mtime;
    ceph::real_time delete_at;
    std::map<std::string, bufferlist> attrs;
  };

  std::string name;
  std::string instance;
  uint version_id{0};
  UUIDPath path;
  Meta meta;
  bool deleted;

  Object(const std::string& _name)
      : name(_name), path(UUIDPath::create()), deleted(false) {}

  Object(const std::string& _name, const uuid_d& _uuid, bool _deleted)
      : name(_name), path(_uuid), deleted(_deleted) {}

  Object(const rgw_obj_key& key)
      : name(key.name),
        instance(key.instance),
        path(UUIDPath::create()),
        deleted(false) {}

  std::filesystem::path get_storage_path() const;

  void metadata_init(
      SFStore* store, const std::string& bucket_id, bool new_object,
      bool new_version
  );
  void metadata_change_version_state(SFStore* store, ObjectState state);
  void metadata_finish(SFStore* store);

  void metadata_flush_attrs(SFStore* store);

  int delete_object_version(SFStore* store);
  void delete_object(SFStore* store);

  bool get_attr(const std::string& name, bufferlist& dest);
};

using ObjectRef = std::shared_ptr<Object>;

/**
 * @brief Represents a part's object.
 *
 * Each part of a multipart upload will have an object associated. This is the
 * object the data is being written to. This class keeps track of the part's
 * state, as well as the object the data is being written to.
 */
struct MultipartObject {
  enum State { NONE, PREPARED, INPROGRESS, DONE, ABORTED };

  ceph::mutex lock = ceph::make_mutex("part_lock");
  ObjectRef objref;
  const std::string upload_id;
  ceph::real_time mtime;
  uint64_t offset;
  uint64_t len;
  std::string etag;
  State state;
  bool aborted;

  MultipartObject(ObjectRef obj, const std::string& _upload_id)
      : objref(obj),
        upload_id(_upload_id),
        state(State::NONE),
        aborted(false) {}

  void finish_write(uint64_t _offset, uint64_t _len, const std::string& _etag) {
    std::lock_guard l(lock);
    if (aborted && state != State::ABORTED) {
      _abort(nullptr);
      return;
    }
    ceph_assert(state != State::DONE);
    state = State::DONE;
    offset = _offset;
    len = _len;
    etag = _etag;
    mtime = ceph::real_clock::now();
  }

  void abort(const DoutPrefixProvider* dpp);

  inline std::string get_cls_name() { return "sfs::multipart_object"; }

 private:
  void _abort(const DoutPrefixProvider* dpp);
};

using MultipartObjectRef = std::shared_ptr<MultipartObject>;

/**
 * @brief Represents a current multipart upload.
 *
 * The MultipartUpload class keeps track of all state pertaining to a given
 * multipart upload, from its start up until it is completed by the user.
 *
 * Each part the user uploads is written to a part file, and all files are
 * aggregated upon completion into one single file representing the final
 * object. Part files are afterwards deleted.
 *
 * Unlike other backends, we do not rely on a meta object to track multipart
 * state.
 *
 * This class expects to have an 'upload_id' provided, which should either be
 * user-specified or generated by the initial caller otherwise. Also expects an
 * sfs::Object to be provided, as it should be the caller's responsibility to
 * deal with object creation/allocation, including version management.
 *
 * At the moment, on-going multipart state is not persisted. Should rgw crash or
 * be stopped, multipart metadata is lost. This is to be addressed in follow up
 * changes. Keep in mind we do not reclaim disk space for a broken multipart.
 * This too shall be addressed.
 */
struct MultipartUpload {
  enum State { NONE, INIT, INPROGRESS, AGGREGATING, DONE, ABORTED };

  const std::string upload_id;
  ACLOwner owner;
  ceph::real_time mtime;
  rgw_placement_rule dest_placement;
  rgw::sal::Attrs attrs;

  State state;
  ceph::mutex parts_map_lock = ceph::make_mutex("parts_map_lock");
  std::map<uint32_t, MultipartObjectRef> parts;
  ObjectRef objref;
  const std::string meta_str;

  MultipartUpload(
      ObjectRef _objref, const std::string& _upload_id, ACLOwner& _owner,
      ceph::real_time _mtime
  )
      : upload_id(_upload_id),
        owner(_owner),
        mtime(_mtime),
        state(State::NONE),
        objref(_objref),
        meta_str("_meta." + _objref->name + "." + _upload_id) {}

  const std::string& get_meta_str() const { return meta_str; }

  const std::string& get_obj_name() const { return objref->name; }

  const std::string& get_upload_id() const { return upload_id; }

  const ACLOwner& get_owner() const { return owner; }

  ceph::real_time& get_mtime() { return mtime; }

  void init(rgw_placement_rule& placement, rgw::sal::Attrs& attrs) {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::NONE);
    state = State::INIT;
    dest_placement = placement;
    this->attrs = attrs;
  }

  MultipartObjectRef get_part(uint64_t part_num) {
    std::lock_guard l(parts_map_lock);

    ceph_assert(state == State::INIT || state == State::INPROGRESS);
    state = State::INPROGRESS;

    auto it = parts.find(part_num);
    if (it != parts.end()) {
      return it->second;
    }
    std::string part_obj_name =
        objref->name + "." + upload_id + ".part." + std::to_string(part_num);
    auto part_obj = std::make_shared<Object>(part_obj_name);
    auto part = std::make_shared<MultipartObject>(part_obj, upload_id);
    parts[part_num] = part;
    return part;
  }

  void aggregate() {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::INPROGRESS);
    state = State::AGGREGATING;
  }

  void finish() {
    std::lock_guard l(parts_map_lock);
    ceph_assert(state == State::AGGREGATING);
    state = State::DONE;
    // drop parts
    parts.clear();
  }

  std::map<uint32_t, MultipartObjectRef> get_parts() {
    std::lock_guard l(parts_map_lock);
    // this is doing a copy.
    return parts;
  }

  void abort(const DoutPrefixProvider* dpp);

  inline std::string get_cls_name() { return "sfs::multipart_upload"; }
};

using MultipartUploadRef = std::shared_ptr<MultipartUpload>;

class Bucket {
  CephContext* cct;
  SFStore* store;
  RGWUserInfo owner;
  RGWBucketInfo info;
  rgw::sal::Attrs attrs;
  bool deleted{false};

 public:
  std::map<std::string, ObjectRef> objects;
  ceph::mutex obj_map_lock = ceph::make_mutex("obj_map_lock");
  ceph::mutex multipart_map_lock = ceph::make_mutex("multipart_map_lock");
  std::map<std::string, MultipartUploadRef> multiparts;

  Bucket(const Bucket&) = default;

 private:
  void _refresh_objects();
  void _undelete_object(
      ObjectRef objref, const rgw_obj_key& key,
      sqlite::SQLiteVersionedObjects& sqlite_versioned_objects,
      sqlite::DBOPVersionedObjectInfo& last_version
  );
  void _finish_object(ObjectRef obj);

 public:
  Bucket(
      CephContext* _cct, SFStore* _store, const RGWBucketInfo& _bucket_info,
      const RGWUserInfo& _owner, const rgw::sal::Attrs& _attrs
  )
      : cct(_cct),
        store(_store),
        owner(_owner),
        info(_bucket_info),
        attrs(_attrs) {
    _refresh_objects();
  }

  const RGWBucketInfo& get_info() const { return info; }

  RGWBucketInfo& get_info() { return info; }

  const rgw::sal::Attrs& get_attrs() const { return attrs; }

  rgw::sal::Attrs& get_attrs() { return attrs; }

  const std::string get_name() const { return info.bucket.name; }

  const std::string get_bucket_id() const { return info.bucket.bucket_id; }

  rgw_bucket& get_bucket() { return info.bucket; }

  const rgw_bucket& get_bucket() const { return info.bucket; }

  RGWUserInfo& get_owner() { return owner; }

  const RGWUserInfo& get_owner() const { return owner; }

  ceph::real_time get_creation_time() const { return info.creation_time; }

  rgw_placement_rule& get_placement_rule() { return info.placement_rule; }

  uint32_t get_flags() const { return info.flags; }

  ObjectRef get_or_create(const rgw_obj_key& key);

  ObjectRef get(const std::string& name) {
    auto it = objects.find(name);
    if (it == objects.end()) {
      throw UnknownObjectException();
    }
    return objects[name];
  }

  void finish(const DoutPrefixProvider* dpp, const std::string& objname);

  void delete_object(ObjectRef objref, const rgw_obj_key& key);

  std::string create_non_existing_object_delete_marker(const rgw_obj_key& key);

  MultipartUploadRef get_multipart(
      const std::string& upload_id, const std::string& oid, ACLOwner owner,
      ceph::real_time mtime
  ) {
    std::lock_guard l(multipart_map_lock);

    auto it = multiparts.find(upload_id);
    if (it != multiparts.end()) {
      auto mp = it->second;
      ceph_assert(mp->objref);
      ceph_assert(mp->objref->name == oid);
      return mp;
    }

    ObjectRef obj = std::make_shared<Object>(oid);
    MultipartUploadRef mp =
        std::make_shared<MultipartUpload>(obj, upload_id, owner, mtime);
    multiparts[upload_id] = mp;
    return mp;
  }

  void finish_multipart(const std::string& upload_id, ObjectRef objref) {
    std::lock_guard l1(obj_map_lock);
    std::lock_guard l2(multipart_map_lock);

    auto it = multiparts.find(upload_id);
    ceph_assert(it != multiparts.end());
    auto mp = it->second;
    mp->finish();
    multiparts.erase(it);

    objref->metadata_finish(store);
    _finish_object(objref);
  }

  std::string gen_multipart_upload_id() {
    auto now = ceph::real_clock::now();
    return ceph::to_iso_8601_no_separators(now, ceph::iso_8601_format::YMDhmsn);
  }

  std::map<std::string, MultipartUploadRef> get_multiparts() {
    std::lock_guard l(multipart_map_lock);
    // this should be doing a copy
    return multiparts;
  }

  void abort_multipart(
      const DoutPrefixProvider* dpp, const std::string& upload_id
  ) {
    std::lock_guard l(multipart_map_lock);
    auto it = multiparts.find(upload_id);
    if (it == multiparts.end()) {
      return;
    }
    multiparts.erase(it);
    it->second->abort(dpp);
  }

  void abort_multiparts(const DoutPrefixProvider* dpp) {
    std::lock_guard l(multipart_map_lock);
    for (const auto& [mpid, mp] : multiparts) {
      mp->abort(dpp);
    }
    multiparts.clear();
  }

  inline std::string get_cls_name() { return "sfs::bucket"; }
};

using BucketRef = std::shared_ptr<Bucket>;

using MetaBucketsRef = std::shared_ptr<sqlite::SQLiteBuckets>;

static inline MetaBucketsRef get_meta_buckets(sqlite::DBConnRef conn) {
  return std::make_shared<sqlite::SQLiteBuckets>(conn);
}

}  // namespace rgw::sal::sfs

#endif  // RGW_STORE_SFS_TYPES_H
