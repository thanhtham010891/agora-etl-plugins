from __future__ import annotations

REDLOCK_ACQUIRE_SCRIPT = """
-- REDLOCK_ACQUIRE
local lease = redis.call("GET", KEYS[1])
if lease ~= false then return {0, false} end
local value = cjson.encode({
    worker_id = ARGV[1],
    acquired_at = ARGV[2],
    pipeline_id = ARGV[3],
    run_number = tonumber(ARGV[4]),
    fencing_token = tonumber(ARGV[6])
})
local ok = redis.call("SET", KEYS[1], value, "NX", "EX", tonumber(ARGV[5]))
if ok then return {1, tonumber(ARGV[6])} end
return {0, false}
"""

REDLOCK_RELEASE_SCRIPT = """
-- REDLOCK_RELEASE
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1]
   and tostring(data["acquired_at"]) == ARGV[2]
   and tostring(data["fencing_token"]) == ARGV[3] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
"""

REDLOCK_RENEW_SCRIPT = """
-- REDLOCK_RENEW
local val = redis.call("GET", KEYS[1])
if val == false then return 0 end
local ok, data = pcall(cjson.decode, val)
if not ok then return 0 end
if data["worker_id"] == ARGV[1]
   and tostring(data["acquired_at"]) == ARGV[2]
   and tostring(data["fencing_token"]) == ARGV[3] then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[4]))
    return 1
end
return 0
"""


__all__ = [
    "REDLOCK_ACQUIRE_SCRIPT",
    "REDLOCK_RELEASE_SCRIPT",
    "REDLOCK_RENEW_SCRIPT",
]
