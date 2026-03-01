/**
 * Clean up Redis Streams for fresh stress test
 */

const redis = new Bun.RedisClient('redis://localhost:6379/0');

async function main() {
  console.log('Cleaning up Redis streams...\n');

  const streams = ['backstage:urgent', 'backstage:default', 'backstage:low'];

  for (const stream of streams) {
    try {
      // Get all consumer groups
      const groups = (await redis.send('XINFO', ['GROUPS', stream])) as any[];
      if (groups && groups.length > 0) {
        console.log(`Stream "${stream}" has ${groups.length} consumer groups`);
        for (const group of groups) {
          const name = group.name;
          console.log(`  Deleting group: ${name}`);
          await redis.send('XGROUP', ['DESTROY', stream, name]);
        }
      }

      // Delete the stream
      const len = await redis.send('XLEN', [stream]);
      console.log(`Deleting stream "${stream}" with ${len} messages...`);
      await redis.send('DEL', [stream]);
    } catch (err: any) {
      if (err.message?.includes('no such key')) {
        console.log(`Stream "${stream}" doesn't exist`);
      } else {
        console.error(`Error with ${stream}:`, err.message);
      }
    }
  }

  console.log('\n✅ Cleanup complete!');
  process.exit(0);
}

main().catch(console.error);
