//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"
#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

namespace ECExtentCache {

  static const uint64_t line_size = 0x1000;
  static const uint64_t pin_mask = line_size - 1;

  void Object::free(Line l)
  {
    cache.erase_stripe(l.address.offset, line_size);

    if (cache.empty())
    {
      thread.objects.erase(oid);
    }
  }

  void Object::request(Read &read)
  {
    if (cache.contains(read.request)) {
      read.backend_complete(cache.intersect(read.request));
      return;
    }

    map<int, extent_set> _request = read.request;
    bool request_empty = true;
    /* else add to read */
    for (auto &&[shard, eset]: _request) {
      eset.subtract(reading[shard]);
      if (!eset.empty()) {
        request_empty = false;
        requesting[shard].insert(eset);
      }
    }

    if(request_empty)
    {
      reading_reads.push_back(read);
    } else {
      requesting_reads.push_back(read);
    }
  }

  void Object::send_reads()
  {
    if (!reading.empty())
      return; // Read busy

    reading.swap(requesting);
    reading_reads.swap(requesting_reads);
    map<hobject_t, map<int, extent_set>> to_read;
    to_read[oid] = requesting;
    thread.backend_read.execute(oid, reading);
  }

  void Object::read_done(shard_extent_map_t buffers)
  {
    cache.insert(buffers);
    reading.clear();

    for (auto &&read: reading_reads)
    {
      read.backend_complete(cache.intersect(read.request));
    }
    reading_reads.clear();

    send_reads();
  }

  void Read::backend_complete(shard_extent_map_t &&_result)
  {
    result = _result;
    // FIXME: Complete back to ECCommon.
  }

  void Thread::complete(Read read)
  {
    for (auto &&[shard, eset]: read.request) {
      for (auto &&[start, len]: eset ) {
        for (uint64_t to_pin = start & ~pin_mask;
            to_pin < start + len;
            to_pin++) {
          Line l = lines.at(Address(read.oid, to_pin));
          if (!--l.ref_count) {
            l.in_lru = true;
            lru.emplace_back(l);
          }
        }
      }
    }
  }

  void Thread::update(hobject_t oid, shard_extent_map_t update)
  {
    objects.at(oid).cache.insert(update);
  }

  void Thread::pin(hobject_t oid, map<int, extent_set> const &request)
  {
    extent_set eset;
    for (auto &&[_, e]: request) eset.insert(e);

    for (auto &&[start, len]: eset )
    {
      for (uint64_t to_pin = start & ~pin_mask;
            to_pin < start + len;
            to_pin++) {
        Address a(oid, to_pin);
        Line l = lines[a];
        if (l.in_lru) lru.remove(l);
        l.in_lru = false;
        l.ref_count++;
      }
    }
  }

  void Thread::batch_complete(hobject_t oid)
  {
    objects.at(oid).send_reads();
  }

  void Thread::free_maybe() {
    while (allocated > size && !lru.empty())
    {
      allocated -= line_size;
      Line &l = lru.front();
      objects.at(l.address.oid).free(l);
      lru.pop_front();
    }
  }

  Read::Read(ReadComplete &read_complete):
    read_complete(read_complete) {}

} // ECExtentCache