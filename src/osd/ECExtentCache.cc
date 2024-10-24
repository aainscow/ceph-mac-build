//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"

#include <rgw/driver/rados/group.h>

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

namespace ECExtentCache {

  uint64_t Object::free(Line &l)
  {
    uint64_t old_size = cache.size();
    cache.erase_stripe(l.address.offset, sinfo->get_chunk_size());

    if (cache.empty()) {
      lru.objects.erase(oid);
    }

    return old_size - cache.size();
  }

  void Object::cache_maybe_ready()
  {
    if (waiting_ops.empty())
      return;

    OpRef op = waiting_ops.front();
    if (cache.contains(op->reads))
    {
      op->result = cache.intersect(op->reads);
      op->complete = true;
      op->cache_ready.cache_ready(op->oid, *op->result);
    }
  }

  void Object::request(OpRef &op)
  {
    /* else add to read */
    if (op->reads) {
      for (auto &&[shard, eset]: *(op->reads)) {
        extent_set request = eset;
        if (cache.contains(shard)) request.subtract(cache.get_extent_map(shard).get_interval_set());
        if (reading.contains(shard)) request.subtract(reading.at(shard));
        if (writing.contains(shard)) request.subtract(writing.at(shard));

        // Store the set of writes we are doing in this IO after subtracting the previous set.
        // We require that the overlapping reads and writes in the requested IO are either read
        // or were written by a previous IO.
        if (op->writes.contains(shard)) writing[shard].insert(op->writes.at(shard));
        if (!request.empty()) {
          requesting[shard].insert(request);
        }
      }
    }

    // Record the writes that will be made by this IO. Future IOs will not need to read this.

    waiting_ops.emplace_back(op);

    cache_maybe_ready();
    send_reads();
  }

  void Object::send_reads()
  {
    if (!reading.empty() || requesting.empty())
      return; // Read busy

    reading.swap(requesting);
    lru.backend_read.backend_read(oid, reading);
  }

  uint64_t Object::read_done(shard_extent_map_t const &buffers)
  {
    reading.clear();
    uint64_t size_change = insert(buffers);
    send_reads();
    return size_change;
  }

  uint64_t Object::write_done(OpRef &op, shard_extent_map_t const &buffers)
  {
    ceph_assert(op == waiting_ops.front());
    waiting_ops.pop_front();
    uint64_t size_change = insert(buffers);
    return size_change;
  }

  uint64_t Object::insert(shard_extent_map_t const &buffers)
  {
    uint64_t old_size = cache.size();
    cache.insert(buffers);
    for (auto && [shard, emap] : buffers.get_extent_maps()) {
      if (writing.contains(shard)) {
        writing.at(shard).subtract(buffers.get_extent_map(shard).get_interval_set());
      }
    }
    cache_maybe_ready();

    return cache.size() - old_size;
  }

  void Lru::request(OpRef &op, hobject_t const &oid, std::optional<std::map<int, extent_set>> const &to_read, std::map<int, extent_set> const &write, stripe_info_t const *sinfo)
  {
    op->oid = oid;
    op->reads = to_read;
    op->writes = write;
    if (!objects.contains(op->oid)) {
      objects.emplace(op->oid, Object(*this, sinfo));
    }
    pin(op);
    objects.at(op->oid).request(op);
  }

  void Lru::read_done(hobject_t const& oid, shard_extent_map_t const&& update)
  {
    size += objects.at(oid).read_done(update);
  }

  void Lru::write_done(OpRef &op, shard_extent_map_t const&& update)
  {
    size += objects.at(op->oid).write_done(op, update);
  }

  void Lru::pin(OpRef &op)
  {
    extent_set eset;
    for (auto &&[_, e]: op->writes) eset.insert(e);
    uint64_t size = objects.at(op->oid).sinfo->get_chunk_size();

    eset.align(size);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += size) {
        Line &l = lines[Address(op->oid, to_pin)];
        if (l.in_lru) lru.remove(l);
        l.in_lru = false;
        l.ref_count++;
      }
    }
  }

  void Lru::complete(OpRef &op)
  {
    extent_set eset;
    for (auto &&[_, e]: op->writes) eset.insert(e);
    uint64_t size = objects.at(op->oid).sinfo->get_chunk_size();
    eset.align(size);

    for (auto &&[start, len]: eset ) {
      for (uint64_t to_pin = start; to_pin < start + len; to_pin += size) {
        Line &l = lines.at(Address(op->oid, to_pin));
        ceph_assert(l.ref_count);
        if (!--l.ref_count) {
          l.in_lru = true;
          lru.emplace_back(l);
        }
      }
    }
    free_maybe();
  }

  void Lru::free_maybe() {
    while (max_size < size && !lru.empty())
    {
      Line &l = lru.front();
      size -= objects.at(l.address.oid).free(l);
      lru.pop_front();
      lines.erase(l.address);
    }
  }

  bool Lru::idle(hobject_t &oid) const
  {
    return objects.contains(oid) && objects.at(oid).waiting_ops.empty();
  }


  Op::Op(CacheReady &read_complete):
    cache_ready(read_complete) {}

} // ECExtentCache