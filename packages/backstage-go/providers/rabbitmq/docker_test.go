package rabbitmq_test

import "os/exec"

// dockerCtl stops or starts a test container for the broker-kill tests:
// plain docker where it runs without root, sudo docker otherwise.
func dockerCtl(action, container string) error {
	if exec.Command("docker", "info").Run() == nil {
		return exec.Command("docker", action, container).Run()
	}
	return exec.Command("sudo", "docker", action, container).Run()
}
