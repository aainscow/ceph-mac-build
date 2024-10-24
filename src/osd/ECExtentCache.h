//
// Created by root on 10/17/24.
//

#ifndef ECEXTENTCACHE_H
#define ECEXTENTCACHE_H

#include "ECUtil.h"

namespace ECExtentCache {

  class Address;
  class Line;
  class Lru;
  class Object;
  class Op;
  typedef std::shared_ptr<Op> OpRef;

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
      return ((std::size_t)a.oid.get_hash()) ^ std::hash<uint64_t>{}(a.offset);
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
    virtual void backend_read(hobject_t oid, std::map<int, extent_set> const &request) = 0;
    virtual ~BackendRead() = default;
  };

  class Lru {
    friend class Op;
    friend class Object;

    unordered_map<Address, Line, AddressHasher> lines;
    std::list<Line> lru;
    uint64_t max_size = 0;
    uint64_t size = 0;
    void free_maybe();

    BackendRead &backend_read;

    void pin(OpRef &op);

  public:
    explicit Lru(BackendRead &backend_read, uint64_t max_size) :
      max_size(max_size), backend_read(backend_read) {}

    std::map<hobject_t, Object> objects;

    // Insert some data into the cache.
    void read_done(hobject_t const& oid, ECUtil::shard_extent_map_t const&& update);
    void write_done(OpRef &op, ECUtil::shard_extent_map_t const&& update);
    void complete(OpRef &read);
    void request(OpRef &op, hobject_t const &oid, std::optional<std::map<int, extent_set>> const &to_read, std::map<int, extent_set> const &write, ECUtil::stripe_info_t const *sinfo);
    bool idle(hobject_t &oid) const;
  };

  struct CacheReady {
    virtual void cache_ready(hobject_t& oid, ECUtil::shard_extent_map_t& result) = 0;

    virtual ~CacheReady() = default;
  };

  class Op
  {
    friend class Object;
    friend class Lru;

    hobject_t oid;
    std::optional<std::map<int, extent_set>> reads;
    std::map<int, extent_set> writes;
    std::optional<ECUtil::shard_extent_map_t> result;
    bool complete = false;
    CacheReady &cache_ready;

  public:
    explicit Op(CacheReady &cache_ready);

    std::optional<ECUtil::shard_extent_map_t> get_result() { return result; }
    std::map<int, extent_set> get_writes() { return writes; }

  };

  class Object
  {

    friend class Lru;
    friend class Op;

    Lru &lru;
    hobject_t oid;
    ECUtil::stripe_info_t const *sinfo;
    std::map<int, extent_set> requesting;
    std::map<int, extent_set> reading;
    std::map<int, extent_set> writing;
    ECUtil::shard_extent_map_t cache;
    std::list<OpRef> waiting_ops;


    uint64_t free(Line &l);
    void request(OpRef &op);
    void send_reads();
    uint64_t read_done(ECUtil::shard_extent_map_t const &result);
    uint64_t write_done(OpRef &op, ECUtil::shard_extent_map_t const &result);
    uint64_t insert(ECUtil::shard_extent_map_t const &buffers);
    void cache_maybe_ready();

  public:
    Object(Lru &thread, ECUtil::stripe_info_t const *sinfo) : lru(thread), sinfo(sinfo), cache(sinfo) {}

  };
} // ECExtentCaches

#endif //ECEXTENTCACHE_H
