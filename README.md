# NeoFS SFTP Gateway
NeoFS SFTP Gateway bridges NeoFS internal protocol and SFTP server.

## Arhival notice

There are no plans currently to continue development of this gateway. But if
you're interested in supporting/evolving it, reach out to NeoSPCC via
info@nspcc.io.

## Installation

```go get -u github.com/nspcc-dev/neofs-sftp-gw```

Or you can call `make` to build it from the cloned repository (the binary will
end up in `bin/neofs-sftp-gw`).

Notable make targets:

```
clean     Clean up
cover     Run tests with race detection and produce coverage output
dep       Pull go dependencies
format    Reformat code
help      Show this help prompt
lint      Run linters
test      Run tests
version   Show current version
```

## Execution
To use this sftp server as OpenSSH subsystem you need to make changes in `/etc/ssh/sshd_config`:
```
# Subsystem     sftp    /usr/lib/openssh/sftp-server
Subsystem       sftp    /path/to/neofs-sftp-gw/bin/neofs-sftp-gw --config /etc/neofs/sftp-gw/config.yaml
```

After that you should restart `sshd`:
``` shell
systemctl restart sshd
```

## Configuration
Sample sftp config:

```
# This section allows you to enable using neofs connections params from user-configs.
# Server changes `${USER}` to user login from variable.
# If enabled, content of the `/home/${USER}/config.yml` file overrides the main section.
user:
  enabled: true
  path: "/home/${USER}/config.yml"



# This main section. It contains params to connect to neofs nodes
wallet:
  path: "/etc/neofs/sftp-gw/wallet.json"
  address:
  passphrase: ""
peers:
  0:
    address: grpcs://s04.neofs.devenv:8082
    weight: 1



# This config section for develop purpose only.
# It enabled the server starts as ssh server (not as openssh subsystem).
dev:
  enabled: false
  sshkey: "~/.ssh/id_ed25519"
  passphrase: "password"
  address: "0.0.0.0:2022"
 
neofs:
  container:
    policy: "REP 3"
```

Sample user config (`/home/${USER}/config.yml`):
```
wallet:
  path: "/home/testuser/wallet.json"
  address: "NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM"
  passphrase: "password"
peers:
  0:
    address: grpcs://s04.neofs.devenv:8082
    weight: 9
  1:
    address: s01.neofs.devenv:8080
    weight: 1
```

## Important notes

- During file uploading, the `neofs-sftp-gw` uses OS TmpDir to store the full file before it is uploaded to NeoFS.
- Uploading a file to the NeoFS requires some time, which is why you should wait until `neofs-sftp-gw` finalizes all actions.
- According to the previous point, significantly increasing the inactivity timeout on the client side is highly recommended.
- Creating dirs (NeoFS containers) is possible, but only the first level. In case of creating dir like "aaa/bbb", the dir `aaa` will be created,
but `bbb` creation will fail with unsupported error.
- By default, container has `acl.Private` rules.

## Known issues

- File overwriting doesn't work. In this case, another file with the same name will be created. In the dir listing, such file will be presented only one time, but it is unknown which one. Dir refreshing will show any version of file.
- File downloading doesn't work.