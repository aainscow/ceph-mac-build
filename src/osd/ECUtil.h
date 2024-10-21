// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECUTIL_H
#define ECUTIL_H

#include <ostream>
#include <opentelemetry/ext/http/client/http_client.h>

#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include "osd_types.h"
#include "ExtentCache.h"

// Setting to 1 turns on very large amounts of level 0 debug containing the
// contents of buffers. Even on level 20 this is not really wanted.
#define DEBUG_EC_BUFFERS 1

namespace ECUtil {
  class shard_extent_map_t;

  class stripe_info_t {
  friend class shard_extent_map_t;

  const uint64_t stripe_width;
  const uint64_t plugin_flags;
  const uint64_t chunk_size;
  const pg_pool_t *pool;
  const int k; // Can be calculated with a division from above. Better to cache.
  const int m;
  const std::vector<int> chunk_mapping;
  const std::map<int, int> chunk_mapping_reverse;

  void ro_range_to_shards(
    uint64_t ro_offset,
    uint64_t ro_size,
    std::map<int, extent_set> *shard_extent_set,
    extent_set *extent_superset,
    buffer::list *bl,
    shard_extent_map_t *shard_extent_map) const;

  static std::vector<int> complete_chunk_mapping(std::vector<int> _chunk_mapping, int n)
  {
    std::vector<int> chunk_mapping(n);
    for (int i=0; i<n; i++) {
      if ((int)_chunk_mapping.size() > i) {
        chunk_mapping[i] = _chunk_mapping.at(i);
      } else {
        chunk_mapping[i] = i;
      }
    }
    return chunk_mapping;
  }

  static std::map<int, int> reverse_chunk_mapping(std::vector<int> _chunk_mapping, int n)
  {
    std::vector<int> chunk_mapping = complete_chunk_mapping(_chunk_mapping, n);
    std::map<int, int> reverse;
    for (int i=0; i<n; i++) {
      reverse[chunk_mapping[i]] = i;
    }
    return reverse;
  }
public:
  stripe_info_t( ErasureCodeInterfaceRef ec_impl, const pg_pool_t *pool, uint64_t stripe_width)
    : stripe_width(stripe_width),
      plugin_flags(ec_impl->get_supported_optimizations()),
      chunk_size(stripe_width / ec_impl->get_data_chunk_count()),
      pool(pool),
      k(ec_impl->get_data_chunk_count()),
      m(ec_impl->get_coding_chunk_count()),
      chunk_mapping(complete_chunk_mapping(ec_impl->get_chunk_mapping(), k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(ec_impl->get_chunk_mapping(), k + m)) {
    ceph_assert(stripe_width % k == 0);
  }
  stripe_info_t( uint64_t k, uint64_t stripe_width, int m, std::vector<int> _chunk_mapping)
    : stripe_width(stripe_width),
      plugin_flags(0),
      chunk_size(stripe_width / k),
      pool(nullptr),
      k(k),
      m(m),
      chunk_mapping(complete_chunk_mapping(_chunk_mapping, k + m)),
      chunk_mapping_reverse(reverse_chunk_mapping(_chunk_mapping, k + m))
  {
    ceph_assert(stripe_width % k == 0);
  }
  bool ro_offset_to_shard_offset(uint64_t ro_offset, int shard) const
  {
    uint64_t full_stripes = (ro_offset / stripe_width) * chunk_size;
    int offset_shard = (ro_offset / chunk_size) % k;

    if (shard == offset_shard) return full_stripes + ro_offset % chunk_size;
    if (shard < offset_shard) return full_stripes + chunk_size;
    return full_stripes;
  }
  bool supports_ec_optimizations() const {
    return pool->allows_ecoptimizations();
  }
  bool supports_ec_overwrites() const {
    return pool->allows_ecoverwrites();
  }
  bool supports_partial_reads() const {
    return (plugin_flags & ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION) != 0;
  }
  bool supports_partial_writes() const {
    return (plugin_flags & ErasureCodeInterface::FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION) != 0;
  }
  bool logical_offset_is_stripe_aligned(uint64_t logical) const {
    return (logical % stripe_width) == 0;
  }
  uint64_t get_stripe_width() const {
    return stripe_width;
  }
  uint64_t get_chunk_size() const {
    return chunk_size;
  }
  int get_m() const {
    return m;
  }
  int get_k() const {
    return k;
  }
  int get_k_plus_m() const {
    return k + m;
  }
  std::vector<int> get_chunk_mapping() const {
    return chunk_mapping;
  }
  int get_shard(int raw_shard) const {
    return chunk_mapping[raw_shard];
  }
  int get_raw_shard(int shard) const
  {
    return chunk_mapping_reverse.at(shard);
  }
  // FIXME: get_k() preferred... but changing would create a big change.
  int get_data_chunk_count() const {
    return k;
  }
  uint64_t logical_to_prev_chunk_offset(uint64_t offset) const {
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t logical_to_next_chunk_offset(uint64_t offset) const {
    return ((offset + stripe_width - 1)/ stripe_width) * chunk_size;
  }
  uint64_t logical_to_prev_stripe_offset(uint64_t offset) const {
    return offset - (offset % stripe_width);
  }
  uint64_t logical_to_next_stripe_offset(uint64_t offset) const {
    return ((offset % stripe_width) ?
      (offset - (offset % stripe_width) + stripe_width) :
      offset);
  }
  uint64_t aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    ceph_assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t chunk_aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    [[maybe_unused]] const auto residue_in_stripe = offset % stripe_width;
    ceph_assert(residue_in_stripe % chunk_size == 0);
    ceph_assert(stripe_width % chunk_size == 0);
    // this rounds down
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t chunk_aligned_logical_size_to_chunk_size(uint64_t len) const {
    // this rounds up
    return ((len + stripe_width - 1) / stripe_width) * chunk_size;
  }
  uint64_t aligned_chunk_offset_to_logical_offset(uint64_t offset) const {
    ceph_assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }
  std::pair<uint64_t, uint64_t> chunk_aligned_offset_len_to_chunk(
    uint64_t off, uint64_t len) const;
  std::pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
    uint64_t _off, uint64_t _len) const {
    uint64_t off = logical_to_prev_stripe_offset(_off);
    uint64_t len = logical_to_next_stripe_offset(
      (_off - off) + _len);
    return std::make_pair(off, len);
  }
  std::pair<uint64_t, uint64_t> offset_len_to_chunk_bounds(
    std::pair<uint64_t, uint64_t> in) const {
    uint64_t off = in.first - (in.first % chunk_size);
    uint64_t tmp_len = (in.first - off) + in.second;
    uint64_t len = ((tmp_len % chunk_size) ?
      (tmp_len - (tmp_len % chunk_size) + chunk_size) :
      tmp_len);
    return std::make_pair(off, len);
  }
  std::pair<uint64_t, uint64_t> offset_len_to_page_bounds(
  std::pair<uint64_t, uint64_t> in) const {
    uint64_t off = in.first - (in.first % CEPH_PAGE_SIZE);
    uint64_t tmp_len = (in.first - off) + in.second;
    uint64_t len = ((tmp_len % CEPH_PAGE_SIZE) ?
      (tmp_len - (tmp_len % CEPH_PAGE_SIZE) + CEPH_PAGE_SIZE) :
      tmp_len);
    return std::make_pair(off, len);
  }
  std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> offset_length_to_data_chunk_extents(
    uint64_t off, uint64_t len) const {
    assert(chunk_size > 0);
    const auto first_chunk_idx = (off / chunk_size);
    const auto last_chunk_idx = (chunk_size - 1 + off + len) / chunk_size;
    const auto first_chunk_offset =  first_chunk_idx * chunk_size;
    const auto first_shard_offset = off - first_chunk_offset + first_chunk_offset/stripe_width;
    const auto last_chunk_len = (len == 0) ? 0:off + len - (last_chunk_idx - 1) * chunk_size;
    return {first_chunk_idx, last_chunk_idx, first_shard_offset, last_chunk_len};
  }
  bool offset_length_is_same_stripe(
    uint64_t off, uint64_t len) const {
    if (len == 0) {
      return true;
    }
    assert(chunk_size > 0);
    const auto first_stripe_idx = off / stripe_width;
    const auto last_inc_stripe_idx = (off + len - 1) / stripe_width;
    return first_stripe_idx == last_inc_stripe_idx;
  }

  void ro_range_to_shard_extent_set(
    uint64_t ro_offset,
    uint64_t ro_size,
    std::map<int, extent_set> &shard_extent_set) const {
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, NULL, NULL, NULL);
  }

  void ro_range_to_shard_extent_set(
    uint64_t ro_offset,
    uint64_t ro_size,
    std::map<int, extent_set> &shard_extent_set,
    extent_set &extent_superset) const {
    ro_range_to_shards(ro_offset, ro_size, &shard_extent_set, &extent_superset, NULL,
                        NULL);
  }

  void ro_range_to_shard_extent_map(
    uint64_t ro_offset,
    uint64_t ro_size,
    buffer::list &bl,
    shard_extent_map_t &shard_extent_map) const {

    ro_range_to_shards(ro_offset, ro_size, NULL, NULL, &bl, &shard_extent_map);
  }
};

inline uint64_t page_mask() {
  static const uint64_t page_mask = ((uint64_t)CEPH_PAGE_SIZE) - 1;
  return page_mask;
}
inline uint64_t align_page_next(uint64_t val) {
  return (val + page_mask()) & ~page_mask();
}
inline uint64_t align_page_prev(uint64_t val) {
  return val & ~page_mask();
}

int decode(
  ErasureCodeInterfaceRef &ec_impl,
  const std::list<std::set<int>> want_to_read,
  const std::list<std::map<int, bufferlist>> chunk_list,
  bufferlist *out);

int decode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  const std::set<int> want_to_read,
  std::map<int, ceph::buffer::list> &to_decode,
  ceph::buffer::list *out);

int decode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  std::map<int, ceph::buffer::list> &to_decode,
  std::map<int, ceph::buffer::list*> &out);

int encode(
  const stripe_info_t &sinfo,
  ceph::ErasureCodeInterfaceRef &ec_impl,
  ceph::buffer::list &in,
  uint64_t offset,
  const std::set<int> &want,
  std::map<int, ceph::buffer::list> *out);

class HashInfo {
  uint64_t total_chunk_size = 0;
  std::vector<uint32_t> cumulative_shard_hashes;
public:
  HashInfo() {}
  explicit HashInfo(unsigned num_chunks) :
    cumulative_shard_hashes(num_chunks, -1) {}
  void append(uint64_t old_size, std::map<int, ceph::buffer::list> &to_append);
  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = std::vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<HashInfo*>& o);
  uint32_t get_chunk_hash(int shard) const {
    ceph_assert((unsigned)shard < cumulative_shard_hashes.size());
    return cumulative_shard_hashes[shard];
  }
  uint64_t get_total_chunk_size() const {
    return total_chunk_size;
  }
  void set_total_chunk_size_clear_hash(uint64_t new_chunk_size) {
    cumulative_shard_hashes.clear();
    total_chunk_size = new_chunk_size;
  }
  bool has_chunk_hash() const {
    return !cumulative_shard_hashes.empty();
  }
  void update_to(const HashInfo &rhs) {
    *this = rhs;
  }
  friend std::ostream& operator<<(std::ostream& out, const HashInfo& hi);
};

typedef std::shared_ptr<HashInfo> HashInfoRef;

class shard_extent_map_t
{
  static const uint64_t invalid_offset = std::numeric_limits<uint64_t>::max();

  const stripe_info_t *sinfo;
  // The maximal range of all extents maps within rados object space.
  uint64_t ro_start;
  uint64_t ro_end;
  std::map<int, extent_map> extent_maps;

  /* This caculates the ro offset for an offset into a particular shard */
  uint64_t calc_ro_offset(int raw_shard, int shard_offset) {
    int stripes = shard_offset / sinfo->chunk_size;
    return stripes * sinfo->stripe_width + raw_shard * sinfo->chunk_size +
      shard_offset % sinfo->chunk_size;
  }

  /* This is a relatively expensive operation to update the ro offset/length.
   * Ideally, we should be able to update offset/length incrementally.
   */
  void compute_ro_range()
  {
    uint64_t start = invalid_offset;
    uint64_t end = 0;

    for (int raw_shard = 0; raw_shard < sinfo->get_data_chunk_count(); ++raw_shard) {
      int shard  = sinfo->get_shard(raw_shard);
      if (extent_maps.contains(shard)) {
        extent_set eset = extent_maps[shard].get_interval_set();
        uint64_t start_iter = calc_ro_offset(raw_shard, eset.range_start());
        if (start_iter < start)
          start = start_iter;

        uint64_t end_iter = calc_ro_offset(raw_shard, eset.range_end()-1)+1;
        if (end_iter > end)
          end = end_iter;
      }
    }
    if (end != 0) {
      ro_start = start;
      ro_end = end;
    } else {
      ro_start = invalid_offset;
      ro_end = invalid_offset;
    }
  }
public:
  shard_extent_map_t(const stripe_info_t *sinfo) :
    sinfo(sinfo),
    ro_start(invalid_offset),
    ro_end(invalid_offset)
  {}

  shard_extent_map_t(const stripe_info_t *sinfo, std::map<int, extent_map> &&extent_maps) :
    sinfo(sinfo),
    extent_maps(std::move(extent_maps))
  {
    // Empty shards are not permitted, so clear them out.
    for (auto iter = extent_maps.begin(); iter != extent_maps.end();) {

      if (iter->second.empty())
        iter = extent_maps.erase(iter);
      else
        ++iter;
    }
    compute_ro_range();
  }

  bool empty() {
    return ro_end == invalid_offset;
  }

  uint64_t get_ro_start()
  {
    return ro_start;
  }

  uint64_t get_ro_end()
  {
    return ro_end;
  }

  /* Return the extent maps.  For reading only, set to const as the returned
   * map should not be modified.
   * We want to avoid:
   *  - Empty extent maps on shards
   *  - getting the offset/length out of sync.
   */
  const std::map<int, extent_map> &get_extent_maps() const {
    return extent_maps;
  }

  /* Return a particlar extent map. This must be const because updating it
   * would cause the shard_extent_map to become inconsistent.
   *
   * * This method will raise an exception if the shard has no extents.
   */
  const extent_map &get_extent_map(int shard) const {
    return extent_maps.at(shard);
  }

  bool contains_shard(int shard) const {
    return extent_maps.contains(shard);
  }

  void erase_after_ro_offset(uint64_t ro_offset);
  shard_extent_map_t intersect_ro_range(uint64_t ro_offset, uint64_t ro_length) const;
  shard_extent_map_t intersect(std::map<int, extent_set> const &other) const;
  void insert_in_shard(int shard, uint64_t off, buffer::list &bl);
  void insert_in_shard(int shard, uint64_t off, buffer::list &bl, uint64_t new_start, uint64_t new_end);
  void insert_ro_zero_buffer( uint64_t ro_offset, uint64_t ro_length );
  void insert(shard_extent_map_t const &other);
  void append_zeros_to_ro_offset( uint64_t ro_offset );
  void insert_ro_extent_map(const extent_map &host_extent_map);
  extent_set get_extent_superset() const;
  int encode(ErasureCodeInterfaceRef& ecimpl, const HashInfoRef &hinfo, uint64_t before_ro_size);
  void decode(ErasureCodeInterfaceRef& ecimpl, std::map<int, extent_set> want);
  void get_buffer(int shard, uint64_t offset, uint64_t length, buffer::list &append_to, bool zero_pad);
  bufferlist get_ro_buffer(uint64_t ro_offset, uint64_t ro_length);
  std::map <int, extent_set> get_extent_set_map();
  void insert_parity_buffers();
  void erase_shard(int shard);
  std::map<int, bufferlist> slice(int offset, int length);
  std::string debug_string(uint64_t inteval, uint64_t offset) const;
  void erase_stripe(uint64_t offset, uint64_t length);
  bool contains(std::map<int, extent_set> const &other) const;

  void assert_buffer_contents_equal(shard_extent_map_t other) const
  {
    for (auto &&[shard, emap]: extent_maps) {
      for (auto &&i : emap) {
        bufferlist bl = i.get_val();
        bufferlist otherbl;
        other.get_buffer(shard, i.get_off(), i.get_len(), otherbl, false);
        ceph_assert(bl.contents_equal(otherbl));
      }
    }
  }

  friend std::ostream& operator<<(std::ostream& lhs, const shard_extent_map_t& rhs);
};


bool is_hinfo_key_string(const std::string &key);
const std::string &get_hinfo_key();

WRITE_CLASS_ENCODER(ECUtil::HashInfo)
}
#endif
