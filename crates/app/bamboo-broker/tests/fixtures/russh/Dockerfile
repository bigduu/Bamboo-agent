FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1

RUN apk add --no-cache \
      netcat-openbsd \
      openssh-server \
      procps \
    && adduser -D -h /home/deploy -s /bin/sh deploy \
    && passwd -d deploy \
    && mkdir -p /home/deploy/.ssh /run/bamboo-russh-fixture /run/sshd \
    && chown -R deploy:deploy /home/deploy \
    && chmod 0700 /home/deploy/.ssh

COPY sshd_config /etc/ssh/sshd_config
COPY entrypoint.sh /usr/local/bin/bamboo-russh-fixture

RUN chmod 0755 /usr/local/bin/bamboo-russh-fixture

EXPOSE 22

HEALTHCHECK --interval=1s --timeout=2s --start-period=1s --retries=20 \
  CMD nc -z -w 1 127.0.0.1 22 || exit 1

ENTRYPOINT ["/usr/local/bin/bamboo-russh-fixture"]
