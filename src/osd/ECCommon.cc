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

#include <iostream>
#include <sstream>

#include "ECCommon.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpRead.h"
#include "ECMsgTypes.h"
#include "PGLog.h"

#include "osd_tracer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::dec;
using std::hex;
using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::bufferptr;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

static ostream& _prefix(std::ostream *_dout, ECCommon::RMWPipeline *rmw_pipeline) {
  return rmw_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout, ECCommon::ReadPipeline *read_pipeline) {
  return read_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout,
			ECCommon::UnstableHashInfoRegistry *unstable_hash_info_registry) {
  // TODO: backref to ECListener?
  return *_dout;
}
static ostream& _prefix(std::ostream *_dout, struct ClientReadCompleter *read_completer);

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::pipeline_state_t &rhs) {
  switch (rhs.pipeline_state) {
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_VALID:
    return lhs << "CACHE_VALID";
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_INVALID:
    return lhs << "CACHE_INVALID";
  default:
    ceph_abort_msg("invalid pipeline state");
  }
  return lhs; // unreachable
}

ostream &operator<<(ostream &lhs, const ECCommon::ec_align_t &rhs)
{
  return lhs << rhs.offset << ","
	     << rhs.size << ","
	     << rhs.flags;
}

ostream &operator<<(ostream &lhs, const ECCommon::ec_extent_t &rhs)
{
  return lhs << rhs.err << ","
	     << rhs.emap;
}

ostream &operator<<(ostream &lhs, const ECCommon::shard_read_t &rhs)
{
  return lhs << "shard_read_t(extents=[" << rhs.extents << "]"
	     << ", subchunk=" << rhs.subchunk
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
	     << ", shard_reads=" << rhs.shard_reads
	     << ", want_attrs=" << rhs.want_attrs
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::read_result_t &rhs)
{
  lhs << "read_result_t(r=" << rhs.r
      << ", errors=" << rhs.errors;
  if (rhs.attrs) {
    lhs << ", attrs=" << *(rhs.attrs);
  } else {
    lhs << ", noattrs";
  }
  return lhs << ", buffers_read=" << rhs.buffers_read << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::ReadOp &rhs)
{
  lhs << "ReadOp(tid=" << rhs.tid;
#ifndef WITH_SEASTAR
  if (rhs.op && rhs.op->get_req()) {
    lhs << ", op=";
    rhs.op->get_req()->print(lhs);
  }
#endif
  return lhs << ", to_read=" << rhs.to_read
	     << ", complete=" << rhs.complete
	     << ", priority=" << rhs.priority
	     << ", obj_to_source=" << rhs.obj_to_source
	     << ", source_to_obj=" << rhs.source_to_obj
	     << ", want_to_read" << rhs.want_to_read
	     << ", in_progress=" << rhs.in_progress << ")";
}

void ECCommon::ReadOp::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
#ifndef WITH_SEASTAR
  if (op && op->get_req()) {
    f->dump_stream("op") << *(op->get_req());
  }
#endif
  f->dump_stream("to_read") << to_read;
  f->dump_stream("complete") << complete;
  f->dump_int("priority", priority);
  f->dump_stream("obj_to_source") << obj_to_source;
  f->dump_stream("source_to_obj") << source_to_obj;
  f->dump_stream("want_to_read") << want_to_read;
  f->dump_stream("in_progress") << in_progress;
}

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
#ifndef WITH_SEASTAR
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
#endif
  lhs << " pg_committed_to=" << rhs.pg_committed_to
      << " temp_added=" << rhs.temp_added
      << " temp_cleared=" << rhs.temp_cleared
      << " pending_read=" << rhs.pending_read
      << " remote_read=" << rhs.remote_read
      << " remote_read_result=" << rhs.remote_read_result
      << " pending_apply=" << rhs.pending_apply
      << " pending_commit=" << rhs.pending_commit
      << " plan.to_read=" << rhs.plan.to_read
      << " plan.will_write=" << rhs.plan.will_write
      << ")";
  return lhs;
}

void ECCommon::ReadPipeline::complete_read_op(ReadOp &rop)
{
  dout(20) << __func__ << " completing " << rop << dendl;
  map<hobject_t, read_request_t>::iterator req_iter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  ceph_assert(rop.to_read.size() == rop.complete.size());
  for (; req_iter != rop.to_read.end(); ++req_iter, ++resiter) {
    ceph_assert(rop.want_to_read.contains(req_iter->first));
    rop.on_complete->finish_single_request(
      req_iter->first,
      resiter->second,
      req_iter->second.to_read,
      rop.want_to_read[req_iter->first]);
  }
  ceph_assert(rop.on_complete);
  std::move(*rop.on_complete).finish(rop.priority);
  rop.on_complete = nullptr;
  // if the read op is over. clean all the data of this tid.
  for (set<pg_shard_t>::iterator iter = rop.in_progress.begin();
    iter != rop.in_progress.end();
    iter++) {
    shard_to_read_map[*iter].erase(rop.tid);
  }
  rop.in_progress.clear();
  tid_to_read_map.erase(rop.tid);
}

void ECCommon::ReadPipeline::on_change()
{
  for (map<ceph_tid_t, ReadOp>::iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    dout(10) << __func__ << ": cancelling " << i->second << dendl;
  }
  tid_to_read_map.clear();
  shard_to_read_map.clear();
  in_progress_client_reads.clear();
}

void ECCommon::ReadPipeline::get_all_avail_shards(
  const hobject_t &hoid,
  const set<pg_shard_t> &error_shards,
  set<int> &have,
  map<shard_id_t, pg_shard_t> &shards,
  bool for_recovery)
{
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (error_shards.find(*i) != error_shards.end())
      continue;
    if (!missing.is_missing(hoid)) {
      ceph_assert(!have.count(i->shard));
      have.insert(i->shard);
      ceph_assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (for_recovery) {
    for (set<pg_shard_t>::const_iterator i =
	   get_parent()->get_backfill_shards().begin();
	 i != get_parent()->get_backfill_shards().end();
	 ++i) {
      if (error_shards.find(*i) != error_shards.end())
	continue;
      if (have.count(i->shard)) {
	ceph_assert(shards.count(i->shard));
	continue;
      }
      dout(10) << __func__ << ": checking backfill " << *i << dendl;
      ceph_assert(!shards.count(i->shard));
      const pg_info_t &info = get_parent()->get_shard_info(*i);
      const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
      if (hoid < info.last_backfill &&
	  !missing.is_missing(hoid)) {
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }

    map<hobject_t, set<pg_shard_t>>::const_iterator miter =
      get_parent()->get_missing_loc_shards().find(hoid);
    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (set<pg_shard_t>::iterator i = miter->second.begin();
	   i != miter->second.end();
	   ++i) {
	dout(10) << __func__ << ": checking missing_loc " << *i << dendl;
	auto m = get_parent()->maybe_get_shard_missing(*i);
	if (m) {
	  ceph_assert(!(*m).is_missing(hoid));
	}
	if (error_shards.find(*i) != error_shards.end())
	  continue;
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }
}

int ECCommon::ReadPipeline::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  vector<shard_read_t> &want_shard_reads,
  bool for_recovery,
  bool do_redundant_reads,
  read_request_t *read_request) {
  // Make sure we don't do redundant reads for recovery
  ceph_assert(!for_recovery || !do_redundant_reads);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  set<int> want;

  for (int i=0; auto &&want_shard_read : want_shard_reads) {
    if (!want_shard_read.extents.empty()) {
      want.insert(i);
    }
    i++;
  }

  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0)
    return r;

  if (do_redundant_reads) {
    vector<pair<int, int>> subchunks_list;
    subchunks_list.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
    for (auto &&i: have) {
      need[i] = subchunks_list;
    }
  }

  if (!read_request)
    return 0;

  bool experimental = cct->_conf->osd_ec_partial_reads_experimental;

  extent_set extra_extents;

  /* First deal with missing shards */
  for (unsigned int i=0; i < want_shard_reads.size(); i++) {
    if (want_shard_reads[i].extents.empty())
      continue;

    /* Work out what extra extents we need to read on each shard. If do
     * redundant reads is set, then we want to have the same reads on
     * every extent. Otherwise, we need to read every shard only if the
     * necessary shard is missing.
     *
     * FIXME: (remove !experimental) This causes every read to grow to to the
     *        superset of all shard reads.  This is required because the
     *        recovery path currently will not re-read shards it has already
     *        read. Once that is fixed, this experimental flag can be removed.
     *
     */
    if (!have.contains(i) || do_redundant_reads || !experimental) {
      extra_extents.union_of(want_shard_reads[i].extents);
    }
  }

  for (auto &&[shard_index, subchunk] : need) {
    if (!have.contains(shard_index)) {
      continue;
    }
    pg_shard_t pg_shard = shards[shard_id_t(shard_index)];
    shard_read_t shard_read;
    shard_read.subchunk = subchunk;
    shard_read.extents.union_of(extra_extents);

    if (shard_index < (int)want_shard_reads.size()) {
      shard_read.extents.union_of(want_shard_reads[shard_index].extents);
    }

    shard_read.extents.align(CEPH_PAGE_SIZE);
    read_request->shard_reads[pg_shard] = shard_read;
  }

  return 0;
}

/*
ASCII Art describing the various variables in the following function:
                    start    end
                      |       |
                      |       |
                      |       |
           - - - - - -v- -+---+-----------+ - - - - - -
                 start_adj|   |           |      ^
to_read.offset - ->-------+   |           | chunk_size
                  |           |           |      v
           +------+ - - - - - + - - - - - + - - - - - -
           |                  |           |
           |                  v           |
           |              - - - - +-------+
           |               end_adj|
           |              +-------+
           |              |       |
           +--------------+       |
                          |       |
                          | shard |
 */
void ECCommon::ReadPipeline::get_min_want_to_read_shards(
    const ec_align_t &to_read,
    const ECUtil::stripe_info_t& sinfo,
    const vector<int>& chunk_mapping,
    vector<shard_read_t> &want_shard_reads) {
  if (to_read.size == 0)
    return;

  uint64_t stripe_width = sinfo.get_stripe_width();
  uint64_t chunk_size = sinfo.get_chunk_size();
  uint64_t data_chunk_count = sinfo.get_data_chunk_count();

  // Aim is to minimise non-^2 divs (chunk_size is assumed to be a power of 2).
  // These should be the only non ^2 divs.
  uint64_t begin_div = to_read.offset / stripe_width;
  uint64_t end_div = (to_read.offset+to_read.size+stripe_width-1)/stripe_width - 1;
  uint64_t start = begin_div*chunk_size;
  uint64_t end = end_div*chunk_size;

  uint64_t start_shard = (to_read.offset - begin_div*stripe_width) / chunk_size;
  uint64_t chunk_count = (to_read.offset + to_read.size + chunk_size - 1)/chunk_size - to_read.offset/chunk_size;;

  // The end_shard needs a modulus to calculate the actual shard, however
  // it is convenient to store it like this for the loop.
  auto end_shard = start_shard + std::min(chunk_count, data_chunk_count);

  // The last shard is the raw shard index which contains the last chunk.
  // Is it possible to calculate this without th e +%?
  uint64_t last_shard = (start_shard + chunk_count - 1) % data_chunk_count;

  for (auto i = start_shard; i < end_shard; i++) {
    auto raw_shard = i >= data_chunk_count ? i - data_chunk_count : i;
    auto shard = chunk_mapping.size() > raw_shard ?
		 chunk_mapping[raw_shard] : static_cast<int>(raw_shard);

    // Adjust the start and end blocks if needed.
    uint64_t start_adj = 0;
    uint64_t end_adj = 0;

    if (raw_shard<start_shard) {
      // Shards before the start, must start on the next chunk.
      start_adj = chunk_size;
    } else if (raw_shard == start_shard) {
      // The start shard itself needs to be moved a partial-chunk forward.
      start_adj = to_read.offset % chunk_size;
    }

    // The end is similar to the start, but the end must be rounded up.
    if (raw_shard < last_shard) {
      end_adj = chunk_size;
    } else if (raw_shard == last_shard) {
      end_adj = (to_read.offset + to_read.size - 1) % chunk_size + 1;
    }

    want_shard_reads[shard].extents.insert(
      start + start_adj,
      end + end_adj - start - start_adj);
  }
}

void ECCommon::ReadPipeline::get_min_want_to_read_shards(
  const ec_align_t &to_read,
  vector<shard_read_t> &want_shard_reads)
{
  get_min_want_to_read_shards(
    to_read, sinfo, ec_impl->get_chunk_mapping(), want_shard_reads);
  dout(20) << __func__ << ": to_read " << to_read
	   << " read_request " << want_shard_reads << dendl;
}

int ECCommon::ReadPipeline::get_remaining_shards(
  const hobject_t &hoid,
  const set<int> &avail,
  const set<int> &want,
  const read_result_t &result,
  map<pg_shard_t, vector<pair<int, int>>> *to_read,
  bool for_recovery)
{
  ceph_assert(to_read);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;
  for (auto &p : result.errors) {
    error_shards.insert(p.first);
  }

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0) {
    dout(0) << __func__ << " not enough shards left to try for " << hoid
	    << " read result was " << result << dendl;
    return -EIO;
  }

  set<int> shards_left;
  for (auto p : need) {
    if (avail.find(p.first) == avail.end()) {
      shards_left.insert(p.first);
    }
  }

  vector<pair<int, int>> subchunks;
  subchunks.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
  for (set<int>::iterator i = shards_left.begin();
       i != shards_left.end();
       ++i) {
    ceph_assert(shards.count(shard_id_t(*i)));
    ceph_assert(avail.find(*i) == avail.end());
    to_read->insert(make_pair(shards[shard_id_t(*i)], subchunks));
  }
  return 0;
}

void ECCommon::ReadPipeline::start_read_op(
  int priority,
  map<hobject_t, set<int>> &want_to_read,
  map<hobject_t, read_request_t> &to_read,
  OpRequestRef _op,
  bool do_redundant_reads,
  bool for_recovery,
  std::unique_ptr<ECCommon::ReadCompleter> on_complete)
{
  ceph_tid_t tid = get_parent()->get_tid();
  ceph_assert(!tid_to_read_map.count(tid));
  auto &op = tid_to_read_map.emplace(
    tid,
    ReadOp(
      priority,
      tid,
      do_redundant_reads,
      for_recovery,
      std::move(on_complete),
      _op,
      std::move(want_to_read),
      std::move(to_read))).first->second;
  dout(10) << __func__ << ": starting " << op << dendl;
  if (_op) {
#ifndef WITH_SEASTAR
    op.trace = _op->pg_trace;
#endif
    op.trace.event("start ec read");
  }
  do_read_op(op);
}

void ECCommon::ReadPipeline::do_read_op(ReadOp &op)
{
  int priority = op.priority;
  ceph_tid_t tid = op.tid;

  dout(10) << __func__ << ": starting read " << op << dendl;

  map<pg_shard_t, ECSubRead> messages;
  for (auto &&[hoid, read_request] : op.to_read) {
    bool need_attrs = read_request.want_attrs;

    for (auto &&[shard, shard_read] : read_request.shard_reads) {
      if (need_attrs) {
	messages[shard].attrs_to_read.insert(hoid);
	need_attrs = false;
      }
      messages[shard].subchunks[hoid] = shard_read.subchunk;
      op.obj_to_source[hoid].insert(shard);
      op.source_to_obj[shard].insert(hoid);
    }
    for (auto &&[shard, shard_read] : read_request.shard_reads) {
      for (auto extent = shard_read.extents.begin();
      		extent != shard_read.extents.end();
		extent++) {
	messages[shard].to_read[hoid].push_back(boost::make_tuple(extent.get_start(), extent.get_len(), read_request.to_read.front().flags));
      }
    }
    ceph_assert(!need_attrs);
  }

  std::vector<std::pair<int, Message*>> m;
  m.reserve(messages.size());
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(
      get_info().pgid.pgid,
      i->first.shard);
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_interval_start_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    if (op.trace) {
      // initialize a child span for this shard
      msg->trace.init("ec sub read", nullptr, &op.trace);
      msg->trace.keyval("shard", i->first.shard.id);
    }
    m.push_back(std::make_pair(i->first.osd, msg));
  }
  if (!m.empty()) {
    get_parent()->send_message_osd_cluster(m, get_osdmap_epoch());
  }

  dout(10) << __func__ << ": started " << op << dendl;
}

void ECCommon::ReadPipeline::get_want_to_read_shards(
  const list<ec_align_t> &to_read,
  std::vector<shard_read_t> &want_shard_reads)
{
  if (cct->_conf->osd_ec_partial_reads) {
      //optimised.
    for (const auto& single_region : to_read) {
      get_min_want_to_read_shards(single_region, want_shard_reads);
    }
    return;
  }

  // Non-optimised version.
  const std::vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
  for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
    int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;

    for (auto &&read : to_read) {
      auto offset_len = sinfo.chunk_aligned_offset_len_to_chunk(read.offset, read.size);
      want_shard_reads[chunk].extents.insert(offset_len.first, offset_len.second);
    }
  }
}

uint64_t ECCommon::ReadPipeline::shard_buffer_list_to_chunk_buffer_list(
  ec_align_t &read,
  std::map<int, extent_map> buffers_read,
  list<map<int, bufferlist>> &chunk_bufferlists,
  list<set<int>> &want_to_reads)
{
  uint64_t chunk_size = sinfo.get_chunk_size();
  int data_chunk_count = sinfo.get_data_chunk_count();
  uint64_t stripe_width = sinfo.get_stripe_width();

  pair read_pair(read.offset, read.size);
  auto aligned_read = sinfo.offset_len_to_page_bounds(read_pair);
  auto chunk_aligned_read = sinfo.offset_len_to_chunk_bounds(read_pair);
  const std::vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();

  int raw_shard = (aligned_read.first / chunk_size) % data_chunk_count;

  for (uint64_t chunk_offset = chunk_aligned_read.first;
      chunk_offset < chunk_aligned_read.first + chunk_aligned_read.second;
      chunk_offset += chunk_size, raw_shard++) {
    if (raw_shard == data_chunk_count) raw_shard = 0;
    auto shard = ((int)chunk_mapping.size()) > raw_shard ?
               chunk_mapping[raw_shard] : raw_shard;

    set<int> want_to_read;
    want_to_read.insert(shard);

    uint64_t sub_chunk_offset = std::max(chunk_offset, aligned_read.first);
    uint64_t sub_chunk_shard_offset = (chunk_offset / stripe_width) * chunk_size + sub_chunk_offset - chunk_offset;
    uint64_t sub_chunk_len = std::min(aligned_read.first + aligned_read.second, chunk_offset + chunk_size) - sub_chunk_offset;
    map<int, bufferlist> chunk_buffers;

    if (buffers_read.contains(shard)) {
      extent_map &emap = buffers_read[shard];
      auto [range, _] = emap.get_containing_range(sub_chunk_shard_offset, sub_chunk_len);

      /* We received a success for this range, so it had better contain the
       * data. */
      ceph_assert(range != emap.end());
      ceph_assert(range.contains(sub_chunk_shard_offset, sub_chunk_len));
      chunk_buffers[shard].substr_of(range.get_val(), sub_chunk_shard_offset - range.get_off(), sub_chunk_len);
    } else for (auto [shardi, emap] : buffers_read) {
      auto [range, _] = emap.get_containing_range(sub_chunk_shard_offset, sub_chunk_len);
      /* EC can often recover without having read every data/coding shard, so
       * ignore the range if the data is missing */
      if (range != emap.end() && range.contains(sub_chunk_shard_offset, sub_chunk_len)) {
        chunk_buffers[shardi].substr_of(range.get_val(), sub_chunk_shard_offset - range.get_off(), sub_chunk_len);
      }
    }
    dout(20) << "decode_prepare: read: (" << read.offset << "~" << read.size << ")" <<
      " aligned: " << aligned_read <<
      " chunk_buffers: " << chunk_buffers <<
      " want_to_read: " << want_to_read << dendl;
    chunk_bufferlists.emplace_back(std::move(chunk_buffers));
    want_to_reads.emplace_back(std::move(want_to_read));
  }

  /* At this point, we could potentially pack multiple chunk decodes into one,
   * as the EC decode methods are able to cope with multiple chunks being
   * decoded at once. Not doing that for now. */

  return read.offset - aligned_read.first;
}

struct ClientReadCompleter : ECCommon::ReadCompleter {
  ClientReadCompleter(ECCommon::ReadPipeline &read_pipeline,
                      ECCommon::ClientAsyncReadStatus *status)
    : read_pipeline(read_pipeline),
      status(status) {}

  void finish_single_request(
    const hobject_t &hoid,
    ECCommon::read_result_t &res,
    list<ECCommon::ec_align_t> to_read,
    set<int> wanted_to_read) override
  {
    auto* cct = read_pipeline.cct;
    dout(20) << __func__ << " completing hoid=" << hoid
             << " res=" << res << " to_read="  << to_read << dendl;
    extent_map result;
    if (res.r != 0)
      goto out;
    ceph_assert(res.errors.empty());

    for (auto &&read: to_read) {
      list<map<int, bufferlist>> chunk_bufferlists;
      list<set<int>> want_to_reads;

      auto off =read_pipeline.shard_buffer_list_to_chunk_buffer_list(
        read, res.buffers_read, chunk_bufferlists, want_to_reads);

      bufferlist bl;
      int r = ECUtil::decode(
	read_pipeline.ec_impl,
	want_to_reads,
	chunk_bufferlists,
	&bl);
      if (r < 0) {
        dout(10) << __func__ << " error on ECUtil::decode r=" << r << dendl;
        res.r = r;
        goto out;
      }
      bufferlist trimmed;
      auto len = std::min(read.size, bl.length() - off);
      dout(20) << __func__ << " bl.length()=" << bl.length()
               << " len=" << len << " read.size=" << read.size
               << " off=" << off << " read.offset=" << read.offset
               << dendl;
      trimmed.substr_of(bl, off, len);
      result.insert(
	read.offset, trimmed.length(), std::move(trimmed));
    }
out:
    dout(20) << __func__ << " calling complete_object with result="
             << result << dendl;
    status->complete_object(hoid, res.r, std::move(result));
    read_pipeline.kick_reads();
  }

  void finish(int priority) && override
  {
    // NOP
  }

  ECCommon::ReadPipeline &read_pipeline;
  ECCommon::ClientAsyncReadStatus *status;
};
static ostream& _prefix(std::ostream *_dout, ClientReadCompleter *read_completer) {
  return _prefix(_dout, &read_completer->read_pipeline);
}

void ECCommon::ReadPipeline::objects_read_and_reconstruct(
  const map<hobject_t, std::list<ECCommon::ec_align_t>> &reads,
  bool fast_read,
  GenContextURef<ECCommon::ec_extents_t &&> &&func)
{
  in_progress_client_reads.emplace_back(
    reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, set<int>> obj_want_to_read;
  set<int> want_to_read;

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&[hoid, to_read]: reads) {
    vector want_shard_reads(ec_impl->get_chunk_count(), shard_read_t());

    get_want_to_read_shards(to_read, want_shard_reads);

    // This is required by the completion.  This currently only contains the
    // relevant shards. We may find this needs the actual relevant extents
    // within the shards, in which case a bigger refactor will be required.
    for (unsigned int i=0; i<want_shard_reads.size(); i++) {
      if (!want_shard_reads[i].extents.empty()) {
        want_to_read.insert(i);
      }
    }

    read_request_t read_request(to_read, false);
    int r = get_min_avail_to_read_shards(
      hoid,
      want_shard_reads,
      false,
      fast_read,
      &read_request);
    ceph_assert(r == 0);

    int subchunk_size =
      sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
    dout(20) << __func__
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(make_pair(hoid, read_request));
    obj_want_to_read.insert(make_pair(hoid, want_to_read));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    obj_want_to_read,
    for_read_op,
    OpRequestRef(),
    fast_read,
    false,
    std::make_unique<ClientReadCompleter>(*this, &(in_progress_client_reads.back())));
}


int ECCommon::ReadPipeline::send_all_remaining_reads(
  const hobject_t &hoid,
  ReadOp &rop)
{
  //FIXME: This function currently assumes that if it has already read a shard
  //       then no further reads from that shard are required.  However with
  //       the experimental optimised partial reads, it is possible for extra
  //       reads to be required to an already-read shard. We plan on fixing this
  //       before allowing such a configuration option to be enabled outside
  //       test/dev environments.
  set<int> already_read;
  const set<pg_shard_t>& ots = rop.obj_to_source[hoid];
  for (set<pg_shard_t>::iterator i = ots.begin(); i != ots.end(); ++i)
    already_read.insert(i->shard);
  dout(10) << __func__ << " have/error shards=" << already_read << dendl;
  map<pg_shard_t, vector<pair<int, int>>> shards;
  int r = get_remaining_shards(hoid, already_read, rop.want_to_read[hoid],
			       rop.complete[hoid], &shards, rop.for_recovery);
  if (r)
    return r;

  list<ec_align_t> to_read = rop.to_read.find(hoid)->second.to_read;

  // (Note cuixf) If we need to read attrs and we read failed, try to read again.
  bool want_attrs =
    rop.to_read.find(hoid)->second.want_attrs &&
    (!rop.complete[hoid].attrs || rop.complete[hoid].attrs->empty());
  if (want_attrs) {
    dout(10) << __func__ << " want attrs again" << dendl;
  }

  read_request_t read_request(to_read, want_attrs);
  for (auto &&[shard, subchunk] : shards) {
    read_request.shard_reads[shard].subchunk = subchunk;

    for (auto &read : to_read) {
      auto p = sinfo.chunk_aligned_offset_len_to_chunk(read.offset, read.size);
      read_request.shard_reads[shard].extents.insert(p.first, p.second);
    }
  }

  rop.to_read.erase(hoid);
  rop.to_read.insert(make_pair(hoid, read_request));
  return 0;
}

void ECCommon::ReadPipeline::kick_reads()
{
  while (in_progress_client_reads.size() &&
         in_progress_client_reads.front().is_complete()) {
    in_progress_client_reads.front().run();
    in_progress_client_reads.pop_front();
  }
}

bool ECCommon::ec_align_t::operator==(const ec_align_t &other) const {
  return offset == other.offset && size == other.size && flags == other.flags;
}

bool ECCommon::shard_read_t::operator==(const shard_read_t &other) const {
  return extents==other.extents && subchunk==other.subchunk;
}

bool ECCommon::read_request_t::operator==(const read_request_t &other) const {
  return to_read == other.to_read &&
    shard_reads == other.shard_reads &&
    want_attrs == other.want_attrs;
}

void ECCommon::RMWPipeline::start_rmw(OpRef op)
{
  ceph_assert(op);
  dout(10) << __func__ << ": " << *op << dendl;

  ceph_assert(!tid_to_op_map.count(op->tid));
  waiting_state.push_back(*op);
  tid_to_op_map[op->tid] = std::move(op);
  check_ops();
}

bool ECCommon::RMWPipeline::try_state_to_reads()
{
  if (waiting_state.empty())
    return false;

  Op *op = &(waiting_state.front());
  if (op->requires_rmw() && pipeline_state.cache_invalid()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    dout(20) << __func__ << ": blocking " << *op
	     << " because it requires an rmw and the cache is invalid "
	     << pipeline_state
	     << dendl;
    return false;
  }

  if (!pipeline_state.caching_enabled()) {
    op->using_cache = false;
  } else if (op->invalidates_cache()) {
    dout(20) << __func__ << ": invalidating cache after this op"
	     << dendl;
    pipeline_state.invalidate();
  }

  waiting_state.pop_front();
  waiting_reads.push_back(*op);

  if (op->using_cache) {
    cache.open_write_pin(op->pin);

    extent_set empty;
    for (auto &&hpair: op->plan.will_write) {
      auto to_read_plan_iter = op->plan.to_read.find(hpair.first);
      const extent_set &to_read_plan =
	to_read_plan_iter == op->plan.to_read.end() ?
	empty :
	to_read_plan_iter->second;

      extent_set remote_read = cache.reserve_extents_for_rmw(
	hpair.first,
	op->pin,
	hpair.second,
	to_read_plan);

      extent_set pending_read = to_read_plan;
      pending_read.subtract(remote_read);

      if (!remote_read.empty()) {
	op->remote_read[hpair.first] = std::move(remote_read);
      }
      if (!pending_read.empty()) {
	op->pending_read[hpair.first] = std::move(pending_read);
      }
    }
  } else {
    op->remote_read = op->plan.to_read;
  }

  dout(10) << __func__ << ": " << *op << dendl;

  if (!op->remote_read.empty()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    objects_read_async_no_cache(
      op->remote_read,
      [op, this](ec_extents_t &&results) {
	for (auto &&i: results) {
	  op->remote_read_result.emplace(make_pair(i.first, i.second.emap));
	}
	check_ops();
      });
  }

  return true;
}

bool ECCommon::RMWPipeline::try_reads_to_commit()
{
  if (waiting_reads.empty())
    return false;
  Op *op = &(waiting_reads.front());
  if (op->read_in_progress())
    return false;
  waiting_reads.pop_front();
  waiting_commit.push_back(*op);

  dout(10) << __func__ << ": starting commit on " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  get_parent()->apply_stats(
    op->hoid,
    op->delta_stats);

  if (op->using_cache) {
    for (auto &&hpair: op->pending_read) {
      op->remote_read_result[hpair.first].insert(
	cache.get_remaining_extents_for_rmw(
	  hpair.first,
	  op->pin,
	  hpair.second));
    }
    op->pending_read.clear();
  } else {
    ceph_assert(op->pending_read.empty());
  }

  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    trans[i->shard];
  }

  op->trace.event("start ec write");

  map<hobject_t,extent_map> written;
  op->generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &written,
    &trans,
    get_parent()->get_dpp(),
    get_osdmap()->require_osd_release);

  dout(20) << __func__ << ": " << cache << dendl;
  dout(20) << __func__ << ": written: " << written << dendl;
  dout(20) << __func__ << ": op: " << *op << dendl;

  if (!get_parent()->get_pool().allows_ecoverwrites()) {
    for (auto &&i: op->log_entries) {
      if (i.requires_kraken()) {
	derr << __func__ << ": log entry " << i << " requires kraken"
	     << " but overwrites are not enabled!" << dendl;
	ceph_abort();
      }
    }
  }

  map<hobject_t,extent_set> written_set;
  for (auto &&i: written) {
    written_set[i.first] = i.second.get_interval_set();
  }
  dout(20) << __func__ << ": written_set: " << written_set << dendl;
  ceph_assert(written_set == op->plan.will_write);

  if (op->using_cache) {
    for (auto &&hpair: written) {
      dout(20) << __func__ << ": " << hpair << dendl;
      cache.present_rmw_update(hpair.first, op->pin, hpair.second);
    }
  }
  op->remote_read.clear();
  op->remote_read_result.clear();

  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(get_parent()->get_acting_recovery_backfill_shards().size());
  set<pg_shard_t> backfill_shards = get_parent()->get_backfill_shards();
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    ceph_assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    const pg_stat_t &stats =
      (should_send || !backfill_shards.count(*i)) ?
      get_info().stats :
      get_parent()->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : empty,
      op->version,
      op->trim_to,
      op->pg_committed_to,
      op->log_entries,
      op->updated_hit_set_history,
      op->temp_added,
      op->temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op->trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op->trace);
      trace.keyval("shard", i->shard.id);
    }

    if (*i == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_osdmap_epoch();
      r->min_epoch = get_parent()->get_interval_start_epoch();
      r->trace = trace;
      messages.push_back(std::make_pair(i->osd, r));
    }
  }

  if (!messages.empty()) {
    get_parent()->send_message_osd_cluster(messages, get_osdmap_epoch());
  }

  if (should_write_local) {
    handle_sub_write(
      get_parent()->whoami_shard(),
      op->client_op,
      local_write_op,
      op->trace);
  }

  for (auto i = op->on_write.begin();
       i != op->on_write.end();
       op->on_write.erase(i++)) {
    (*i)();
  }

  return true;
}

struct ECDummyOp : ECCommon::RMWPipeline::Op {
  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ecimpl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t,extent_map> *written,
      std::map<shard_id_t, ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const ceph_release_t require_osd_release) final
  {
    // NOP, as -- in constrast to ECClassicalOp -- there is no
    // transaction involved
  }
};

bool ECCommon::RMWPipeline::try_finish_rmw()
{
  if (waiting_commit.empty())
    return false;
  Op *op = &(waiting_commit.front());
  if (op->write_in_progress())
    return false;
  waiting_commit.pop_front();

  dout(10) << __func__ << ": " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  if (op->pg_committed_to > completed_to)
    completed_to = op->pg_committed_to;
  if (op->version > committed_to)
    committed_to = op->version;

  if (get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
    if (op->version > get_parent()->get_log().get_can_rollback_to() &&
	waiting_reads.empty() &&
	waiting_commit.empty()) {
      // submit a dummy, transaction-empty op to kick the rollforward
      auto tid = get_parent()->get_tid();
      auto nop = std::make_unique<ECDummyOp>();
      nop->hoid = op->hoid;
      nop->trim_to = op->trim_to;
      nop->pg_committed_to = op->version;
      nop->tid = tid;
      nop->reqid = op->reqid;
      waiting_reads.push_back(*nop);
      tid_to_op_map[tid] = std::move(nop);
    }
  }

  if (op->using_cache) {
    cache.release_write_pin(op->pin);
  }
  tid_to_op_map.erase(op->tid);

  if (waiting_reads.empty() &&
      waiting_commit.empty()) {
    pipeline_state.clear();
    dout(20) << __func__ << ": clearing pipeline_state "
	     << pipeline_state
	     << dendl;
  }
  return true;
}

void ECCommon::RMWPipeline::check_ops()
{
  while (try_state_to_reads() ||
	 try_reads_to_commit() ||
	 try_finish_rmw());
}

void ECCommon::RMWPipeline::on_change()
{
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  pipeline_state.clear();
  waiting_reads.clear();
  waiting_state.clear();
  waiting_commit.clear();
  for (auto &&op: tid_to_op_map) {
    cache.release_write_pin(op.second->pin);
  }
  tid_to_op_map.clear();
}

void ECCommon::RMWPipeline::call_write_ordered(std::function<void(void)> &&cb) {
  if (!waiting_state.empty()) {
    waiting_state.back().on_write.emplace_back(std::move(cb));
  } else if (!waiting_reads.empty()) {
    waiting_reads.back().on_write.emplace_back(std::move(cb));
  } else {
    // Nothing earlier in the pipeline, just call it
    cb();
  }
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::maybe_put_hash_info(
  const hobject_t &hoid,
  ECUtil::HashInfo &&hinfo)
{
  return registry.lookup_or_create(hoid, hinfo);
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::get_hash_info(
  const hobject_t &hoid,
  bool create,
  const map<string, bufferlist, less<>>& attrs,
  uint64_t size)
{
  dout(10) << __func__ << ": Getting attr on " << hoid << dendl;
  ECUtil::HashInfoRef ref = registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    bufferlist bl;
    map<string, bufferlist>::const_iterator k = attrs.find(ECUtil::get_hinfo_key());
    if (k == attrs.end()) {
      dout(5) << __func__ << " " << hoid << " missing hinfo attr" << dendl;
    } else {
      bl = k->second;
    }
    if (bl.length() > 0) {
      auto bp = bl.cbegin();
      try {
        decode(hinfo, bp);
      } catch(...) {
        dout(0) << __func__ << ": Can't decode hinfo for " << hoid << dendl;
        return ECUtil::HashInfoRef();
      }
      if (hinfo.get_total_chunk_size() != size) {
        dout(0) << __func__ << ": Mismatch of total_chunk_size "
      		       << hinfo.get_total_chunk_size() << dendl;
        return ECUtil::HashInfoRef();
      } else {
        create = true;
      }
    } else if (size == 0) { // If empty object and no hinfo, create it
      create = true;
    }
    if (create) {
      ref = registry.lookup_or_create(hoid, hinfo);
    }
  }
  return ref;
}
