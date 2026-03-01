/**
 * Debug script to check Redis Streams behavior
 */

const redis = new Bun.RedisClient('redis://localhost:6379/0');

async function main() {
  const streamKey = 'backstage:default';
  const groupName = `debug-group-${Date.now()}`;
  const consumerId = 'debug-consumer';

  console.log('\n=== REDIS STREAMS DEBUG ===\n');

  // Check stream length
  try {
    const len = await redis.send('XLEN', [streamKey]);
    console.log(`Stream "${streamKey}" length: ${len}`);
  } catch (err) {
    console.log(`Stream doesn't exist yet`);
  }

  // Add a test message
  console.log('\nAdding test message...');
  const msgId = await redis.send('XADD', [
    streamKey,
    '*',
    'taskName',
    'debug.test',
    'payload',
    '{"test": true}',
    'enqueuedAt',
    String(Date.now()),
  ]);
  console.log(`Added message: ${msgId}`);

  // Create consumer group
  console.log(`\nCreating consumer group "${groupName}"...`);
  try {
    await redis.send('XGROUP', [
      'CREATE',
      streamKey,
      groupName,
      '0',
      'MKSTREAM',
    ]);
    console.log('Consumer group created (starting from 0)');
  } catch (err: any) {
    if (err.message?.includes('BUSYGROUP')) {
      console.log('Group already exists');
    } else {
      throw err;
    }
  }

  // Try to read with >
  console.log('\nReading with ">" (new messages)...');
  const result1 = await redis.send('XREADGROUP', [
    'GROUP',
    groupName,
    consumerId,
    'COUNT',
    '10',
    'STREAMS',
    streamKey,
    '>',
  ]);
  console.log('Result with ">":', JSON.stringify(result1, null, 2));

  // Check group info
  console.log('\nConsumer groups info:');
  const groups = await redis.send('XINFO', ['GROUPS', streamKey]);
  console.log(JSON.stringify(groups, null, 2));

  // Try reading with 0 (pending)
  console.log('\nReading with "0" (pending entries)...');
  const result2 = await redis.send('XREADGROUP', [
    'GROUP',
    groupName,
    consumerId,
    'COUNT',
    '10',
    'STREAMS',
    streamKey,
    '0',
  ]);
  console.log('Result with "0":', JSON.stringify(result2, null, 2));

  // Clean up
  console.log('\nDeleting consumer group...');
  await redis.send('XGROUP', ['DESTROY', streamKey, groupName]);
  console.log('Done!');

  process.exit(0);
}

main().catch(console.error);
