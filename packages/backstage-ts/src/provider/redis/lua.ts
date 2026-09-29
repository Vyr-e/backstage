export const PROCESS_SCHEDULED_LUA = `
local zsetKey = KEYS[1]
local cutoff = tonumber(ARGV[1])
local prefix = ARGV[2]
local defaultPriority = ARGV[3]

local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local processed = 0

for _, taskData in ipairs(tasks) do
    local ok, task = pcall(cjson.decode, taskData)
    if ok and task then
        local streamKey = task.streamKey or (prefix .. ':' .. (task.priority or defaultPriority))
        
        local args = {streamKey, '*', 'taskName', task.taskName or '', 'payload', task.payload or '{}', 'enqueuedAt', tostring(task.enqueuedAt or 0)}
        
        if task.attempts then
            table.insert(args, 'attempts')
            table.insert(args, tostring(task.attempts))
        end
        if task.backoff then
            table.insert(args, 'backoff')
            table.insert(args, task.backoff)
        end
        if task.timeout then
            table.insert(args, 'timeout')
            table.insert(args, tostring(task.timeout))
        end
        
        redis.call('XADD', unpack(args))
        redis.call('ZREM', zsetKey, taskData)
        processed = processed + 1
    end
end

return processed
`;

/** Move due members to claimed ZSET for cross-provider delays. */
export const CLAIM_SCHEDULED_LUA = `
local zsetKey = KEYS[1]
local claimedKey = KEYS[2]
local cutoff = tonumber(ARGV[1])
local tasks = redis.call('ZRANGEBYSCORE', zsetKey, '-inf', cutoff)
local claimed = {}
for _, taskData in ipairs(tasks) do
  redis.call('ZADD', claimedKey, cutoff, taskData)
  redis.call('ZREM', zsetKey, taskData)
  table.insert(claimed, taskData)
end
return claimed
`;
