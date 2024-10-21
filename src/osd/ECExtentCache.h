//
// Created by root on 10/17/24.
//

#ifndef ECEXTENTCACHE_H
#define ECEXTENTCACHE_H

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

namespace ECExtentCache {

  class Address;
  class Line;
  class Thread;
  class Object;
  class Read;

  class Address
  {
  public:
    hobject_t oid;
    uint64_t offset;

    friend bool operator==(const Address& lhs, const Address& rhs)
    {
      return lhs.oid == rhs.oid
        && lhs.offset == rhs.offset;
    }

    friend bool operator!=(const Address& lhs, const Address& rhs)
    {
      return !(lhs == rhs);
    }
  };

  struct AddressHasher
  {
    std::size_t operator()(const Address& a) const
    {
      return (()std::size_t)a.oid.get_hash()) ^ std::hash<uint64_t>(a.offset);
    }
  };

  class Line
  {
  public:
    bool in_lru;
    int ref_count;
    Address address;

    friend bool operator==(const Line& lhs, const Line& rhs)
    {
      return lhs.in_lru == rhs.in_lru
        && lhs.ref_count == rhs.ref_count
        && lhs.address == rhs.address;
    }

    friend bool operator!=(const Line& lhs, const Line& rhs)
    {
      return !(lhs == rhs);
    }
  };

  struct BackendRead {
    virtual void execute(hobject_t oid, map<int, extent_set> const &request) = 0;
    virtual ~BackendRead() {}
  };

  class Thread {
    friend class Read;
    friend class Object;

    unordered_map<Address, Line, AddressHasher> lines;
    list<Line> lru;
    uint64_t allocated;
    uint64_t size;
    void free_maybe();

    BackendRead &backend_read;

  public:
    Thread(BackendRead &backend_read) : backend_read(backend_read) {}

    map<hobject_t, Object> objects;

    void pin(hobject_t oid, map<int, extent_set>  const&request);
    void batch_complete(hobject_t oid);
    void update(hobject_t oid, shard_extent_map_t update);
    void complete(Read read);

  };

  struct ReadComplete {
    virtual void execute(Read read) = 0;

    virtual ~ReadComplete() {}
  };

  template<typename T>
  struct read_complete : BackendRead, private T {
    read_complete(T l) noexcept : T{l} {}

    auto execute() -> int override {
      return T::operator()();
    }
  };

  class Read
  {
    friend class Object;
    friend class Thread;

    hobject_t oid;
    map<int, extent_set> request;
    optional<shard_extent_map_t> result;

    void backend_complete(shard_extent_map_t &&result);
    ReadComplete &read_complete;

  public:
    Read(ReadComplete &read_complete);
  };

  class Object
  {

    friend class Thread;
    friend class Read;

    Thread &thread;
    hobject_t oid;
    map<int, extent_set> requesting;
    map<int, extent_set> reading;
    shard_extent_map_t cache;
    list<Read> requesting_reads;
    list<Read> reading_reads;

    void free(Line l);
    void request(Read &read);
    void send_reads();
    void read_done(shard_extent_map_t result);

  public:
    Object(Thread &thread, stripe_info_t *sinfo) : thread(thread), cache(sinfo) {}

  };
} // ECExtentCache

#endif //ECEXTENTCACHE_H
