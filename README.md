# HyperProxy

HyperProxy is a cross-platform TCP tunneling solution that connects clients and servers using public key identification over a DHT network called [HyperSwarm](https://github.com/holepunchto/hyperdht). It consists of two main components:

1. A TypeScript-based command-line client+server running on Node.js
2. An Android client developed with React Native

## Key Features

- **Public Key Identification**: Clients identify servers using public keys, not IP addresses or hostnames.
- **TCP Tunneling**: Create secure tunnels between clients and servers.
- **Cross-Platform Compatibility**: Works across different platforms via command-line interface and Android app.
- **HyperSwarm Integration**: Utilizes the HyperSwarm DHT network for decentralized connectivity.

## Command Line Usage

Here's a simple example of how to use the command line client:

```bash
# Start the daemon
node index.ts daemon

# Show the key
node index.ts show key
boss     canoe    marble   proof
blue     salmon   distance shy
lemon    useless  library  thing
manual   nose     february tank
seek     term     offer    build
monster  panther  come     boring

# Add a service on port 12345 with the name "ServiceName"
node index.ts add 12345 Service name

# Allow connections from a specific public key (BIP39 format)
node index.ts allow turn process will element clock obey forward talk remain frown cupboard march pear inmate remove trip reunion budget monkey furnace nurse tobacco donkey utility
```

On the client:

```bash
# Start the daemon in client mode
node index.ts daemon noserve

# Show the key
node index.ts show key
turn     process  will     element
clock    obey     forward  talk
remain   frown    cupboard march
pear     inmate   remove   trip
reunion  budget   monkey   furnace
nurse    tobacco  donkey   utility

# Register a server with its public key
node index.ts register MyServer boss canoe marble proof blue salmon distance shy lemon useless library thing manual nose february tank seek term offer build monster panther come boring

# List available services on the registered server
node index.ts list MyServer
Service name

# Connect to a service on the registered server, mapping it to local port 54321
node index.ts port 54321 connect MyServer Service name

# Any connection to port 54321 on the client will now be forwarded to port 12345 on the server
```

For full command-line documentation, see the [Usage](##usage) section below.

## Android Client

The Android client provides the same functionality as the command line but with a user-friendly interface. It also includes a button to open local ports connected to server services in your browser for easy web access.

## License

All code in this project is available under the permissive [0BSD license](https://opensource.org/licenses/0BSD). However, some dependencies are licensed under other licenses like MIT and Apache 2. Please review the licenses of any dependencies you use.

---

# Usage (Command Line)

```bash
Commands:
  daemon [noserve]
    Start the daemon and listen for connections and commands
    If "noserve" is specified, the daemon does not listen to client
    connections, and may be used purely as a client
    Note: None of the other commands will work of a daemon has not been started
  show key
    Show the public key
  show services
    Show the list of available services
  show allowed
    Show the list of allowed remote public keys
  show servers
    Show the list of registered servers
  show proxies
    Show the list of active proxies
  add [<host>] <port> <name>
    Add the specified service to the list of services offered by this server
    If host is not specified, localhost is used
  remove <name>
    Remove the specified service from the service list
  allow <public key>
    Add the specified public key to the list of clients allowed to
    connect to this server
  deny <public key>
    Remove the specified public key from the list of clients allowed to
    connect to this server
  register <name> <public key>
    Register the specified public key as a new server
  unregister <name>
    Remove the specified server from the registered servers list
  list <server name>
    List service names of services offered by the specified server
  [port <port>] connect <server name> <service name>
    Connect to the specified service on the specified server and proxy
    any connections to a local port to that service
    If a port is not specified, a random local port is used
  disconnect <port>
    Disconnect a previously established connection to a server
  licenses
    Display licenses of libraries used in this software
```
