package io.inbot.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.util.Pool;
import redis.clients.util.Slowlog;

/**
 * Sub class of Jedis that does nothing.
 */
@SuppressWarnings("deprecation")
public class FakeJedis extends Jedis {
    private static final Logger LOG = LoggerFactory.getLogger(FakeJedis.class);

    private boolean verbose;

    private final Map<String,Object> store=new ConcurrentHashMap<>();

    public FakeJedis() {
        super("localhost");
    }

    public void verbose(boolean on) {
        this.verbose=on;
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return null;
    }

    @Override
    public Long append(String key, String value) {
        return null;
    }

    @Override
    public String asking() {
        return null;
    }

    @Override
    public String auth(String password) {
        return null;
    }

    @Override
    public String bgrewriteaof() {
        return null;
    }

    @Override
    public String bgsave() {
        return null;
    }

    @Override
    public Long bitcount(byte[] key) {
        return null;
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return null;
    }

    @Override
    public Long bitcount(String key) {
        return null;
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return null;
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        return null;
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        return null;
    }

    @Override
    public Long bitpos(byte[] key, boolean value) {
        return null;
    }

    @Override
    public Long bitpos(byte[] key, boolean value, BitPosParams params) {
        return null;
    }

    @Override
    public Long bitpos(String key, boolean value) {
        return null;
    }

    @Override
    public Long bitpos(String key, boolean value, BitPosParams params) {
        return null;
    }

    @Override
    public List<byte[]> blpop(byte[] arg) {
        return null;
    }

    @Override
    public List<byte[]> blpop(byte[]... args) {
        return null;
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return null;
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return null;
    }

    @Override
    public List<String> blpop(String arg) {
        return null;
    }

    @Override
    public List<String> blpop(String... args) {
        return null;
    }

    @Override
    public List<byte[]> brpop(byte[] arg) {
        return null;
    }

    @Override
    public List<byte[]> brpop(byte[]... args) {
        return null;
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return null;
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return null;
    }

    @Override
    public List<String> brpop(String arg) {
        return null;
    }

    @Override
    public List<String> brpop(String... args) {
        return null;
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        return null;
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return null;
    }

    @Override
    public String clientGetname() {
        return null;
    }

    @Override
    public String clientKill(byte[] client) {
        return null;
    }

    @Override
    public String clientKill(String client) {
        return null;
    }

    @Override
    public String clientList() {
        return null;
    }

    @Override
    public String clientSetname(byte[] name) {
        return null;
    }

    @Override
    public String clientSetname(String name) {
        return null;
    }

    @Override
    public void close() {
        // since super constructor may create stuff that needs releasing, call super.close
        super.close();
    }

    @Override
    public String clusterAddSlots(int... slots) {
        return null;
    }

    @Override
    public Long clusterCountKeysInSlot(int slot) {
        return null;
    }

    @Override
    public String clusterDelSlots(int... slots) {
        return null;
    }

    @Override
    public String clusterFailover() {
        return null;
    }

    @Override
    public String clusterFlushSlots() {
        return null;
    }

    @Override
    public String clusterForget(String nodeId) {
        return null;
    }

    @Override
    public List<String> clusterGetKeysInSlot(int slot, int count) {
        return null;
    }

    @Override
    public String clusterInfo() {
        return null;
    }

    @Override
    public Long clusterKeySlot(String key) {
        return null;
    }

    @Override
    public String clusterMeet(String ip, int port) {
        return null;
    }

    @Override
    public String clusterNodes() {
        return null;
    }

    @Override
    public String clusterReplicate(String nodeId) {
        return null;
    }

    @Override
    public String clusterSaveConfig() {
        return null;
    }

    @Override
    public String clusterSetSlotImporting(int slot, String nodeId) {
        return null;
    }

    @Override
    public String clusterSetSlotMigrating(int slot, String nodeId) {
        return null;
    }

    @Override
    public String clusterSetSlotNode(int slot, String nodeId) {
        return null;
    }

    @Override
    public String clusterSetSlotStable(int slot) {
        return null;
    }

    @Override
    public List<String> clusterSlaves(String nodeId) {
        return null;
    }

    @Override
    public List<byte[]> configGet(byte[] pattern) {
        return null;
    }

    @Override
    public List<String> configGet(String pattern) {
        return null;
    }

    @Override
    public String configResetStat() {
        return null;
    }

    @Override
    public byte[] configSet(byte[] parameter, byte[] value) {
        return null;
    }

    @Override
    public String configSet(String parameter, String value) {
        return null;
    }

    @Override
    public void connect() {
    }

    @Override
    public Long dbSize() {
        return null;
    }

    @Override
    public String debug(DebugParams params) {
        return null;
    }

    @Override
    public Long decr(byte[] key) {
        return null;
    }

    @Override
    public Long decr(String key) {
        return null;
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        return null;
    }

    @Override
    public Long decrBy(String key, long integer) {
        return null;
    }

    @Override
    public Long del(byte[] key) {
        return null;
    }

    @Override
    public Long del(byte[]... keys) {
        return null;
    }

    @Override
    public Long del(String key) {
        return null;
    }

    @Override
    public Long del(String... keys) {
        return null;
    }

    @Override
    public void disconnect() {
    }

    @Override
    public byte[] dump(byte[] key) {
        return null;
    }

    @Override
    public byte[] dump(String key) {
        return null;
    }

    @Override
    public byte[] echo(byte[] string) {
        return null;
    }

    @Override
    public String echo(String string) {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public Object eval(byte[] script) {
        return null;
    }

    @Override
    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
        return null;
    }

    @Override
    public Object eval(byte[] script, int keyCount, byte[]... params) {
        return null;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return null;
    }

    @Override
    public Object eval(String script) {
        return null;
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        return null;
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return null;
    }

    @Override
    public Object evalsha(byte[] sha1) {
        return null;
    }

    @Override
    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
        return null;
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        return null;
    }

    @Override
    public Object evalsha(String script) {
        return null;
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        return "";
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        return null;
    }

    @Override
    public Boolean exists(byte[] key) {
        return false;
    }

    @Override
    public Boolean exists(String key) {
        return false;
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return null;
    }

    @Override
    public Long expire(String key, int seconds) {
        return null;
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return null;
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return null;
    }

    @Override
    public String flushAll() {
        return null;
    }

    @Override
    public String flushDB() {
        return null;
    }

    @Override
    public byte[] get(byte[] key) {
        return null;
    }

    @Override
    public String get(String key) {
        Object value = store.get(key);
        if(value==null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return false;
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return false;
    }

    @Override
    public Client getClient() {
        return null;
    }

    @Override
    public Long getDB() {
        return null;
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        return null;
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return null;
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return null;
    }

    @Override
    public String getSet(String key, String value) {
        return null;
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        return null;
    }

    @Override
    public Long hdel(String key, String... fields) {
        return null;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return false;
    }

    @Override
    public Boolean hexists(String key, String field) {
        return false;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String,String> getOrCreateMap(String key) {
        Object object = store.get(key);
        if(object == null) {
            Map<String, String> theMap = new ConcurrentHashMap<>();
            store.put(key, theMap);
            return theMap;
        } else {
            return (Map<String, String>) object;
        }
    }

    @Override
    public String hget(String key, String field) {
        return getOrCreateMap(key).get(field);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return null;
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return Collections.unmodifiableMap(getOrCreateMap(key));
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return null;
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return null;
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return null;
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        return null;
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return null;
    }

    @Override
    public Set<String> hkeys(String key) {
        return getOrCreateMap(key).keySet();
    }

    @Override
    public Long hlen(byte[] key) {
        return null;
    }

    @Override
    public Long hlen(String key) {
        return (long)getOrCreateMap(key).size();
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return null;
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        List<String> results= new ArrayList<>();
        for(String f: fields) {
            results.add(hget(key, f));
        }
        return results;
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return null;
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        Map<String, String> m = getOrCreateMap(key);
        for(Entry<String, String> e: hash.entrySet()) {
            m.put(e.getKey(), e.getValue());
        }
        return "OK";
    }

    @Override
    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
        return null;
    }

    @Override
    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        return null;
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
        return null;
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, int cursor, ScanParams params) {
        return null;
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
        return null;
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        return null;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return null;
    }

    @Override
    public Long hset(String key, String field, String value) {
        Map<String, String> m = getOrCreateMap(key);
        m.put(field, value);
        return null;
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return null;
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return null;
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        return null;
    }

    @Override
    public List<String> hvals(String key) {
        return null;
    }

    @Override
    public Long incr(byte[] key) {
        return null;
    }

    @Override
    public Long incr(String key) {
        return null;
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        return null;
    }

    @Override
    public Long incrBy(String key, long integer) {
        return null;
    }

    @Override
    public Double incrByFloat(byte[] key, double integer) {
        return null;
    }

    @Override
    public Double incrByFloat(String key, double value) {
        return null;
    }

    @Override
    public String info() {
        return null;
    }

    @Override
    public String info(String section) {
        return null;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return null;
    }

    @Override
    public Set<String> keys(String pattern) {
        return null;
    }

    @Override
    public Long lastsave() {
        return null;
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return null;
    }

    @Override
    public String lindex(String key, long index) {
        return null;
    }

    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return null;
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return null;
    }

    @Override
    public Long llen(byte[] key) {
        return null;
    }

    @Override
    public Long llen(String key) {
        return null;
    }

    @Override
    public byte[] lpop(byte[] key) {
        return null;
    }

    @Override
    public String lpop(String key) {
        return null;
    }

    @Override
    public Long lpush(byte[] key, byte[]... strings) {
        return -1l;
    }

    @Override
    public Long lpush(String key, String... strings) {
        return -1l;
    }

    @Override
    public Long lpushx(byte[] key, byte[]... string) {
        return -1l;
    }

    @Override
    public Long lpushx(String key, String... string) {
        return -1l;
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        return null;
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return null;
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return null;
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return null;
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        return null;
    }

    @Override
    public String lset(String key, long index, String value) {
        return null;
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        return null;
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return null;
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        return null;
    }

    @Override
    public List<String> mget(String... keys) {
        return null;
    }

    @Override
    public String migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
        return null;
    }

    @Override
    public String migrate(String host, int port, String key, int destinationDb, int timeout) {
        return null;
    }

    @Override
    public void monitor(JedisMonitor jedisMonitor) {
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        return null;
    }

    @Override
    public Long move(String key, int dbIndex) {
        return null;
    }

    @Override
    public String mset(byte[]... keysvalues) {
        return null;
    }

    @Override
    public String mset(String... keysvalues) {
        return null;
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        return null;
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return null;
    }

    @Override
    public Transaction multi() {
        return null;
    }

    @Override
    public List<Object> multi(TransactionBlock jedisTransaction) {
        return null;
    }

    @Override
    public byte[] objectEncoding(byte[] key) {
        return null;
    }

    @Override
    public String objectEncoding(String string) {
        return null;
    }

    @Override
    public Long objectIdletime(byte[] key) {
        return null;
    }

    @Override
    public Long objectIdletime(String string) {
        return null;
    }

    @Override
    public Long objectRefcount(byte[] key) {
        return null;
    }

    @Override
    public Long objectRefcount(String string) {
        return null;
    }

    @Override
    public Long persist(byte[] key) {
        return null;
    }

    @Override
    public Long persist(String key) {
        return null;
    }

    @Override
    public Long pexpire(byte[] key, int milliseconds) {
        return null;
    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        return null;
    }

    @Override
    public Long pexpire(String key, int milliseconds) {
        return null;
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return null;
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        return null;
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return null;
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        return null;
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return null;
    }

    @Override
    public long pfcount(byte[] key) {
        return 0;
    }

    @Override
    public Long pfcount(byte[]... keys) {
        return null;
    }

    @Override
    public long pfcount(String key) {
        return 0;
    }

    @Override
    public long pfcount(String... keys) {
        return 0;
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        return null;
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        return null;
    }

    @Override
    public String ping() {
        return null;
    }

    @Override
    public Pipeline pipelined() {
        return null;
    }

    @Override
    public List<Object> pipelined(PipelineBlock jedisPipeline) {
        return null;
    }

    @Override
    public String psetex(byte[] key, int milliseconds, byte[] value) {
        return null;
    }

    @Override
    public String psetex(String key, int milliseconds, String value) {
        return null;
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
    }

    @Override
    public Long pttl(byte[] key) {
        return null;
    }

    @Override
    public Long pttl(String key) {
        return null;
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        return null;
    }

    @Override
    public Long publish(String channel, String message) {
        return null;
    }

    @Override
    public List<String> pubsubChannels(String pattern) {
        return null;
    }

    @Override
    public Long pubsubNumPat() {
        return null;
    }

    @Override
    public Map<String, String> pubsubNumSub(String... channels) {
        return null;
    }

    @Override
    public String quit() {
        return null;
    }

    @Override
    public byte[] randomBinaryKey() {
        return null;
    }

    @Override
    public String randomKey() {
        return null;
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return null;
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return null;
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return null;
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return null;
    }

    @Override
    public void resetState() {
    }

    @Override
    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        return null;
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        return null;
    }

    @Override
    public byte[] rpop(byte[] key) {
        return null;
    }

    @Override
    public String rpop(String key) {
        return null;
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        return null;
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return null;
    }

    @Override
    public Long rpush(byte[] key, byte[]... strings) {
        return null;
    }

    @Override
    public Long rpush(String key, String... strings) {
        if(verbose) {
            LOG.info("rpush key: {}" + StringUtils.join(strings));
        }
        return null;
    }

    @Override
    public Long rpushx(byte[] key, byte[]... string) {
        return null;
    }

    @Override
    public Long rpushx(String key, String... string) {
        return null;
    }

    @Override
    public Long sadd(byte[] key, byte[]... members) {
        return null;
    }

    @Override
    public Long sadd(String key, String... members) {
        return null;
    }

    @Override
    public String save() {
        return null;
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor) {
        return null;
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        return null;
    }

    @Override
    public ScanResult<String> scan(int cursor) {
        return null;
    }

    @Override
    public ScanResult<String> scan(int cursor, ScanParams params) {
        return null;
    }

    @Override
    public ScanResult<String> scan(String cursor) {
        return null;
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        return null;
    }

    @Override
    public Long scard(byte[] key) {
        return null;
    }

    @Override
    public Long scard(String key) {
        return null;
    }

    @Override
    public List<Long> scriptExists(byte[]... sha1) {
        return null;
    }

    @Override
    public Boolean scriptExists(String sha1) {
        return false;
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        return null;
    }

    @Override
    public String scriptFlush() {
        return null;
    }

    @Override
    public String scriptKill() {
        return null;
    }

    @Override
    public byte[] scriptLoad(byte[] script) {
        return null;
    }

    @Override
    public String scriptLoad(String script) {
        return null;
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return null;
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return null;
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return null;
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return null;
    }

    @Override
    public String select(int index) {
        return null;
    }

    @Override
    public String sentinelFailover(String masterName) {
        return null;
    }

    @Override
    public List<String> sentinelGetMasterAddrByName(String masterName) {
        return null;
    }

    @Override
    public List<Map<String, String>> sentinelMasters() {
        return null;
    }

    @Override
    public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
        return null;
    }

    @Override
    public String sentinelRemove(String masterName) {
        return null;
    }

    @Override
    public Long sentinelReset(String pattern) {
        return null;
    }

    @Override
    public String sentinelSet(String masterName, Map<String, String> parameterMap) {
        return null;
    }

    @Override
    public List<Map<String, String>> sentinelSlaves(String masterName) {
        return null;
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return null;
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx) {
        return null;
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
        return null;
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
        return null;
    }

    @Override
    public String set(String key, String value) {
        return null;
    }

    @Override
    public String set(String key, String value, String nxxx) {
        return null;
    }

    @Override
    public String set(String key, String value, String nxxx, String expx, int time) {
        return null;
    }

    @Override
    public String set(String key, String value, String nxxx, String expx, long time) {
        return null;
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return false;
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return false;
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return false;
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return false;
    }

    @Override
    public void setDataSource(Pool<Jedis> jedisPool) {
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return null;
    }

    @Override
    public String setex(String key, int seconds, String value) {
        store.put(key, value);
        return value;
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return null;
    }

    @Override
    public Long setnx(String key, String value) {
        return null;
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        return null;
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return null;
    }

    @Override
    public String shutdown() {
        return null;
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        return null;
    }

    @Override
    public Set<String> sinter(String... keys) {
        return null;
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return null;
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return null;
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return false;
    }

    @Override
    public Boolean sismember(String key, String member) {
        return false;
    }

    @Override
    public String slaveof(String host, int port) {
        return null;
    }

    @Override
    public String slaveofNoOne() {
        return null;
    }

    @Override
    public List<Slowlog> slowlogGet() {
        return null;
    }
}

