// Broker-kill tests stop and start the test containers. Plain `docker` where
// it works without root (Docker Desktop, OrbStack), `sudo docker` otherwise.
const needsSudo = (await Bun.$`docker info`.quiet().nothrow()).exitCode !== 0;

export async function dockerCtl(
  action: 'stop' | 'start',
  container: string,
): Promise<void> {
  if (needsSudo) await Bun.$`sudo docker ${action} ${container}`.quiet();
  else await Bun.$`docker ${action} ${container}`.quiet();
}
