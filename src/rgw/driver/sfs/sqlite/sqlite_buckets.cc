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
#include "sqlite_buckets.h"

#include <driver/sfs/sqlite/buckets/bucket_definitions.h>
#include <driver/sfs/sqlite/users/users_definitions.h>

#include "objects/object_definitions.h"
#include "retry.h"
#include "versioned_object/versioned_object_definitions.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteBuckets::SQLiteBuckets(DBConnRef _conn) : conn(_conn) {}

std::vector<DBOPBucketInfo> get_rgw_buckets(
    const std::vector<DBBucket>& db_buckets
) {
  std::vector<DBOPBucketInfo> ret_buckets;
  for (const auto& db_bucket : db_buckets) {
    auto rgw_bucket = get_rgw_bucket(db_bucket);
    ret_buckets.push_back(rgw_bucket);
  }
  return ret_buckets;
}

std::optional<DBOPBucketInfo> SQLiteBuckets::get_bucket(
    const std::string& bucket_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  auto bucket = storage->get_pointer<DBBucket>(bucket_id);
  std::optional<DBOPBucketInfo> ret_value;
  if (bucket) {
    ret_value = get_rgw_bucket(*bucket);
  }
  return ret_value;
}

std::optional<std::pair<std::string, std::string>> SQLiteBuckets::get_owner(
    const std::string& bucket_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  const auto rows = storage->select(
      columns(&DBUser::user_id, &DBUser::display_name),
      inner_join<DBUser>(on(is_equal(&DBBucket::owner_id, &DBUser::user_id))),
      where(is_equal(&DBBucket::bucket_id, bucket_id))
  );
  if (rows.size() == 0) {
    return std::nullopt;
  }
  const auto row = rows[0];
  return std::make_pair(std::get<0>(row), std::get<1>(row).value_or(""));
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_bucket_by_name(
    const std::string& bucket_name, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return get_rgw_buckets(
      storage->get_all<DBBucket>(where(c(&DBBucket::bucket_name) = bucket_name))
  );
}

void SQLiteBuckets::store_bucket(
    const DBOPBucketInfo& bucket, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  auto db_bucket = get_db_bucket(bucket);
  storage->replace(db_bucket);
}

void SQLiteBuckets::remove_bucket(
    const std::string& bucket_name, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  storage->remove<DBBucket>(bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids(
    rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return storage->select(&DBBucket::bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids(
    const std::string& user_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return storage->select(
      &DBBucket::bucket_name, where(c(&DBBucket::owner_id) = user_id)
  );
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets(
    rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return get_rgw_buckets(storage->get_all<DBBucket>());
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets(
    const std::string& user_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return get_rgw_buckets(
      storage->get_all<DBBucket>(where(c(&DBBucket::owner_id) = user_id))
  );
}

std::vector<std::string> SQLiteBuckets::get_deleted_buckets_ids(
    rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  return storage->select(
      &DBBucket::bucket_id, where(c(&DBBucket::deleted) = true)
  );
}

bool SQLiteBuckets::bucket_empty(
    const std::string& bucket_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  auto num_ids = storage->count<DBVersionedObject>(
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_equal(&DBVersionedObject::version_type, VersionType::REGULAR)
      )
  );
  return num_ids == 0;
}

std::optional<DBDeletedObjectItems> SQLiteBuckets::delete_bucket_transact(
    const std::string& bucket_id, uint max_objects, bool& bucket_deleted,
    rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  RetrySQLiteBusy<DBDeletedObjectItems> retry([&]() {
    bucket_deleted = false;
    DBDeletedObjectItems ret_values;
    auto transaction = storage->transaction_guard();
    // first get all the objects and versions for that bucket
    ret_values = storage->select(
        columns(&DBObject::uuid, &DBVersionedObject::id),
        inner_join<DBObject>(
            on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
        ),
        where(is_equal(&DBObject::bucket_id, bucket_id)),
        order_by(&DBVersionedObject::size).desc(), limit(max_objects)
    );
    for (auto const& uuid_version : ret_values) {
      // remove the versions first
      storage->remove<DBVersionedObject>(std::get<1>(uuid_version));
      // try to delete the object (it will throw an exception if it's not
      // empty yet)
      // possible errors when object is not empty are:
      // SQLITE_CONSTRAINT: legacy sqlite error
      // SQLITE_CONSTRAINT_FOREIGNKEY: extended sqlite error
      try {
        storage->remove<DBObject>(std::get<0>(uuid_version));
      } catch (const std::system_error& e) {
        if (e.code().value() != SQLITE_CONSTRAINT_FOREIGNKEY &&
            e.code().value() != SQLITE_CONSTRAINT) {
          throw(e);
        }
      }
    }
    // try to delete the bucket
    try {
      storage->remove<DBBucket>(bucket_id);
      bucket_deleted = true;
    } catch (const std::system_error& e) {
      if (e.code().value() != SQLITE_CONSTRAINT_FOREIGNKEY &&
          e.code().value() != SQLITE_CONSTRAINT) {
        throw(e);
      }
    }
    transaction.commit();
    return ret_values;
  });
  return retry.run();
}

const std::optional<SQLiteBuckets::Stats> SQLiteBuckets::get_stats(
    const std::string& bucket_id, rgw::sal::sfs::sqlite::StorageRef storage
) const {
  if (!storage) {
    storage = conn->get_storage();
  }
  std::optional<SQLiteBuckets::Stats> stats;

  auto res = storage->select(
      columns(
          count(&DBVersionedObject::object_id), sum(&DBVersionedObject::size)
      ),
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED)
      )
  );

  if (res.size() > 0) {
    ceph_assert(res.size() == 1);
    stats = SQLiteBuckets::Stats();
    stats->obj_count = std::get<0>(res[0]);
    // We get a unique_ptr for SUM(), likely because of the underlying type of 'size'.
    // Therefore we need to check whether it's a nullptr or not.
    auto size = std::get<1>(res[0]).get();
    stats->size = (size ? *size : 0);
  }

  return stats;
}

}  // namespace rgw::sal::sfs::sqlite
