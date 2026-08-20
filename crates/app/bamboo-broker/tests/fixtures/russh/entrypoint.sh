#!/bin/sh
set -eu

authorized_key=/run/bamboo-russh-fixture/authorized_key.pub

if [ ! -s "$authorized_key" ]; then
  echo "fixture public key is missing or empty" >&2
  exit 1
fi

cp "$authorized_key" /home/deploy/.ssh/authorized_keys
chown deploy:deploy /home/deploy/.ssh/authorized_keys
chmod 0600 /home/deploy/.ssh/authorized_keys

# Generate a fresh host identity for every container so the test exercises TOFU
# and pinning rather than inheriting a baked-in key.
rm -f /etc/ssh/ssh_host_ed25519_key /etc/ssh/ssh_host_ed25519_key.pub
ssh-keygen -q -t ed25519 -N '' -f /etc/ssh/ssh_host_ed25519_key

/usr/sbin/sshd -t -f /etc/ssh/sshd_config
exec /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config
