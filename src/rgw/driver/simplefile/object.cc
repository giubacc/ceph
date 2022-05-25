// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "rgw_sal_simplefile.h"
#include "driver/simplefile/object.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SimpleFileObject::SimpleFileReadOp::SimpleFileReadOp(SimpleFileObject *_source,
                                                     RGWObjectCtx *_rctx)
    : source(_source), rctx(_rctx) {}

int SimpleFileObject::SimpleFileReadOp::prepare(
  optional_yield y,
  const DoutPrefixProvider *dpp
) {

  source->refresh_meta();

  auto objspath = source->store->objects_path(source->bucket->get_key());
  auto objdata = objspath / source->get_name();
  const std::string metafn = "_meta." + source->get_name();
  auto objmeta = objspath / metafn;

  if (!std::filesystem::exists(objmeta)) {
    lsfs_dout(dpp, 10) << "object metadata not found at " << objmeta << dendl;
    return -ENOENT;
  } else if (!std::filesystem::exists(objdata)) {
    lsfs_dout(dpp, 10) << "object data not found at " << objdata << dendl;
    return -ENOENT;
  }

  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size() << dendl;
  return 0;
}

int SimpleFileObject::SimpleFileReadOp::get_attr(const DoutPrefixProvider *dpp,
                                                 const char *name,
                                                 bufferlist &dest,
                                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO: " << name << dendl;

  if (std::strcmp(name, "user.rgw.acl") == 0) {
    // TODO support 'user.rgw.acl' to support read_permissions. Return
    // empty policy since our test user is admin for now.
    RGWAccessControlPolicy policy;
    policy.encode(dest);
    return 0;
  }
  return -ENOTSUP;
}

// sync read
int SimpleFileObject::SimpleFileReadOp::read(int64_t ofs, int64_t end,
                                             bufferlist &bl, optional_yield y,
                                             const DoutPrefixProvider *dpp) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs
                     << ", end: " << end
                     << ", len: " << len << dendl;

  auto objpath =
    source->store->objects_path(source->bucket->get_key()) / source->get_name();
  ceph_assert(std::filesystem::exists(objpath));

  std::string error;
  int ret = bl.pread_file(objpath.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objpath
                       << ". Returning EIO." << dendl;
    return -EIO;
  }
  return 0;
}

// async read
int SimpleFileObject::SimpleFileReadOp::iterate(const DoutPrefixProvider *dpp,
                                                int64_t ofs, int64_t end,
                                                RGWGetDataCB *cb,
                                                optional_yield y) {
  // TODO bounds check, etc.
  const auto len = end + 1 - ofs;
  lsfs_dout(dpp, 10) << "bucket: " << source->bucket->get_name()
                     << ", obj: " << source->get_name()
                     << ", size: " << source->get_obj_size()
                     << ", offset: " << ofs
                     << ", end: " << end
                     << ", len: " << len << dendl;

  auto objpath =
    source->store->objects_path(source->bucket->get_key()) / source->get_name();
  ceph_assert(std::filesystem::exists(objpath));

  // TODO chunk the read
  bufferlist bl;
  std::string error;
  int ret = bl.pread_file(objpath.c_str(), ofs, len, &error);
  if (ret < 0) {
    lsfs_dout(dpp, 10) << "failed to read object from file " << objpath
                       << ". Returning EIO." << dendl;
    return -EIO;
  }

  cb->handle_data(bl, ofs, len);
  return 0;
}

SimpleFileObject::SimpleFileDeleteOp::SimpleFileDeleteOp(
    SimpleFileObject *_source)
    : source(_source) {}

int SimpleFileObject::SimpleFileDeleteOp::delete_obj(
    const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_object(const DoutPrefixProvider *dpp,
                                    optional_yield y, bool prevent_versioning) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_obj_aio(const DoutPrefixProvider *dpp,
                                     RGWObjState *astate, Completions *aio,
                                     bool keep_index_consistent,
                                     optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::copy_object(
    User *user, req_info *info, const rgw_zone_id &source_zone,
    rgw::sal::Object *dest_object, rgw::sal::Bucket *dest_bucket,
    rgw::sal::Bucket *src_bucket, const rgw_placement_rule &dest_placement,
    ceph::real_time *src_mtime, ceph::real_time *mtime,
    const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
    bool high_precision_time, const char *if_match, const char *if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs &attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string *version_id,
    std::string *tag, std::string *etag, void (*progress_cb)(off_t, void *),
    void *progress_data, const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

/** TODO Create a randomized instance ID for this object */
void SimpleFileObject::gen_rand_obj_instance_name() {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileObject::get_obj_attrs(optional_yield y,
                                    const DoutPrefixProvider *dpp,
                                    rgw_obj *target_obj) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::modify_obj_attrs(const char *attr_name,
                                       bufferlist &attr_val, optional_yield y,
                                       const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::delete_obj_attrs(const DoutPrefixProvider *dpp,
                                       const char *attr_name,
                                       optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

std::unique_ptr<MPSerializer> SimpleFileObject::get_serializer(
  const DoutPrefixProvider *dpp, const std::string &lock_name) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

int SimpleFileObject::transition(Bucket *bucket,
                                 const rgw_placement_rule &placement_rule,
                                 const real_time &mtime, uint64_t olh_epoch,
                                 const DoutPrefixProvider *dpp,
                                 optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::transition_to_cloud(
  Bucket* bucket,
  rgw::sal::PlacementTier* tier,
  rgw_bucket_dir_entry& o,
  std::set<std::string>& cloud_targets,
  CephContext* cct,
  bool update_object,
  const DoutPrefixProvider* dpp,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": not supported" << dendl;
  return -ENOTSUP;
}

bool SimpleFileObject::placement_rules_match(rgw_placement_rule &r1,
                                             rgw_placement_rule &r2) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return true;
}

int SimpleFileObject::dump_obj_layout(const DoutPrefixProvider *dpp,
                                      optional_yield y, Formatter *f) {
  ldout(store->ceph_context(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::swift_versioning_restore(bool &restored, /* out */
                                               const DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SimpleFileObject::swift_versioning_copy(const DoutPrefixProvider *dpp,
                                            optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": do nothing." << dendl;
  return 0;
}

int SimpleFileObject::omap_get_vals(const DoutPrefixProvider *dpp,
                                    const std::string &marker, uint64_t count,
                                    std::map<std::string, bufferlist> *m,
                                    bool *pmore, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}
int SimpleFileObject::omap_get_all(const DoutPrefixProvider *dpp,
                                   std::map<std::string, bufferlist> *m,
                                   optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                            const std::string &oid,
                                            const std::set<std::string> &keys,
                                            Attrs *vals) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                          const std::string &key,
                                          bufferlist &val, bool must_exist,
                                          optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileObject::chown(rgw::sal::User &new_user,
                            const DoutPrefixProvider *dpp,
                            optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

void SimpleFileObject::write_meta() {
  auto b = static_cast<SimpleFileBucket*>(bucket);
  std::string metafn = "_meta." + get_name();
  auto metapath = b->objects_path() / metafn;

  ofstream ofs(metapath);
  JSONFormatter f(true);
  f.open_object_section("meta");
  encode_json("meta", meta, &f);
  f.close_section();  // meta
  f.flush(ofs);
  ofs.close();
}

void SimpleFileObject::load_meta() {
  auto b = static_cast<SimpleFileBucket*>(bucket);
  std::string metafn = "_meta." + get_name();
  auto metapath = b->objects_path() / metafn;

  ldout(store->ctx(), 10) << "load metadata for " << get_name() << dendl;

  if (!std::filesystem::exists(metapath)) {
    ldout(store->ctx(), 10) << "unable to find meta for object " << get_name()
                       << " at " << metapath << dendl;
    return;
  }

  JSONParser parser;
  bool res = parser.parse(metapath.c_str());
  ceph_assert(res);

  auto it = parser.find("meta");
  ceph_assert(!it.end());

  JSONDecoder::decode_json("meta", meta, &parser);

  set_obj_size(meta.size);
  set_attrs(meta.attrs);
}

} // ns rgw::sal