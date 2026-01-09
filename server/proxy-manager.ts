import type DHT from 'hyperdht';
import {
	createServer,
} from 'node:net';
import {
	Abort,
} from './pin-stream/abort.ts';
import {
	ChangeListener,
} from './pin-stream/change.ts';
import {
	createClosedWritePin,
	createPipe,
	sendFinish,
	sendValue,
} from './pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from './pin-stream/pin-stream-compat.ts';
import {
	runProcesses,
} from './pin-stream/processes.ts';
import {
	streamSplitter,
} from './pin-stream/stream-splitter.ts';
import type {
	StreamSplitterStream,
} from './pin-stream/stream-splitter.ts';
import {
	waitTimeout,
} from './pin-stream/timeout.ts';
import {
	Unrace,
} from './pin-stream/unrace.ts';
import {
	sendServerRequest,
} from './protocol.ts';

export interface ActiveProxy {
	port: number;
	serverName: string;
	serviceName: Uint8Array;
}

interface ActiveProxyImpl extends ActiveProxy {
	abort: Abort;
}

const activeProxiesByServer = new Map<string, Set<ActiveProxyImpl>>();
const activeProxiesByPort = new Map<number, ActiveProxyImpl>();

export async function addProxy(node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverName: string, serverKey: Uint8Array, serviceName: Uint8Array, port: number): Promise<number> {
	const proxy: ActiveProxyImpl = {
		port,
		serverName,
		serviceName,
		abort: new Abort(),
	};
	if (port !== 0 && activeProxiesByPort.has(port)) {
		throw new Error(`Port ${ port } already in use by another proxy`);
	}
	proxy.port = await createProxy(node, keyPair, serverKey, serviceName, port, proxy.abort);
	if (activeProxiesByPort.has(proxy.port)) {
		proxy.abort.abort();
		throw new Error(`Port ${ proxy.port } already in use by another proxy`);
	}
	activeProxiesByPort.set(proxy.port, proxy)
	if (!activeProxiesByServer.has(serverName)) {
		activeProxiesByServer.set(serverName, new Set());
	}
	activeProxiesByServer.get(serverName)!.add(proxy);
	return proxy.port;
}

export function getActiveProxies(): ActiveProxy[] {
	return [...activeProxiesByPort.values()].sort((a, b) => (a.port - b.port));
}

export function removeProxyByPort(port: number): void {
	const proxy = activeProxiesByPort.get(port);
	if (!proxy) {
		return;
	}
	activeProxiesByPort.delete(port);
	removeProxy(proxy);
}

export function removeProxiesByServer(serverName: string): void {
	const toRemove = activeProxiesByServer.get(serverName);
	if (!toRemove) {
		return;
	}
	activeProxiesByServer.delete(serverName);
	for (const proxy of toRemove) {
		removeProxy(proxy);
	}
}

function removeProxy(proxy: ActiveProxyImpl): void {
	proxy.abort.abort();
	const serverProxies = activeProxiesByServer.get(proxy.serverName);
	if (serverProxies) {
		serverProxies.delete(proxy);
		if (serverProxies.size < 1) {
			activeProxiesByServer.delete(proxy.serverName);
		}
	}
	if (activeProxiesByPort.get(proxy.port) === proxy) {
		activeProxiesByPort.delete(proxy.port);
	}
}

async function createProxy(node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverKey: Uint8Array, serviceName: Uint8Array, port: number, abort: Abort): Promise<number> {
	const { readPin: localStreamsReadPin, writePin: localStreamsWritePin } = createPipe<StreamSplitterStream>();
	const localStreamsUnrace = new Unrace();

	(async function retry() {
		const remote = node.connect(serverKey, { keyPair });
		remote.on('error', (error: unknown) => {
			// The default behavior for `EventEmitter` is to crash the VM if an error
			// is emitted without a handler. This handler pre-emptively prevents this
			console.log('Proxy socket error', error);
		});
		// Manage connection to server
		try {
			await runProcesses(async (processes, optionsProc) => {
				const { readPin, writePin: joinedOutput } = createPipe<Uint8Array>();
				const { readPin: joinedInput, writePin } = createPipe<Uint8Array>();

				await sendValue(processes, async (options) => {
					await processReadable(remote, writePin, options);
				}, optionsProc);

				await sendValue(processes, async (options) => {
					await processWritable(readPin, remote, options);
				}, optionsProc);

				await sendValue(processes, async (options) => {
					// First send request
					await sendServerRequest(joinedOutput, {
						type: 'proxy',
						serviceName,
					}, options);

					// Then treat the rest of the stream as part of the stream splitter
					await streamSplitter(joinedInput, joinedOutput, localStreamsReadPin, createClosedWritePin<StreamSplitterStream>(), options);
				}, optionsProc);

				await sendFinish(processes, optionsProc);
			}, {
				abort,
			});
		}
		finally {
			if (!abort.aborted) {
				// Connection failed or unexpectedly disconnected
				(async () => {
					// Wait 5 seconds
					await waitTimeout(5000, {
						abort,
					});
					// Spawn another connection
					retry().catch((error: unknown) => {
						if (!abort.aborted) {
							// Unexpected disconnection is logged here
							console.log(error);
						}
					});
				})().catch((error: unknown) => {
					if (!abort.aborted) {
						// Should not arrive here, since there's nothing else that could throw
						// Log an error anyway
						console.log(error);
					}
				});
			}
		}
	})().catch((error: unknown) => {
		if (!abort.aborted) {
			// Unexpected disconnection is logged here
			console.log(error);
		}
	});

	// Listen for incoming connections
	const server = createServer();
	server.on('error', (error: unknown) => {
		// The default behavior for `EventEmitter` is to crash the VM if an error
		// is emitted without a handler. This handler pre-emptively prevents this
		console.log('Listener error', error);
	});
	server.on('connection', (socket) => {
		socket.on('error', (error: unknown) => {
			// The default behavior for `EventEmitter` is to crash the VM if an error
			// is emitted without a handler. This handler pre-emptively prevents this
			console.log('Incoming socket error', error);
		});
		// Incoming connection to local socket
		(async () => {
			const localAbort = new Abort();
			try {
				await runProcesses(async (processes, optionsProc = {}) => {
					const {
						abort: remoteAbort,
					} = optionsProc;

					const { readPin, writePin: socketWritePin } = createPipe<Uint8Array>();
					const { readPin: socketReadPin, writePin } = createPipe<Uint8Array>();

					await sendValue(processes, async (options) => {
						await processReadable(socket, socketWritePin, options);
					}, optionsProc);

					await sendValue(processes, async (options) => {
						await processWritable(socketReadPin, socket, options);
					}, optionsProc);

					// Send this socket as a new local connection to the stream splitter
					await localStreamsUnrace.run(async () => {
						await sendValue(localStreamsWritePin, {
							readPin,
							writePin,
							localAbort,
							remoteAbort,
						}, optionsProc);
					}, optionsProc);
				}, {
					abort,
				});
			}
			catch (error) {
				localAbort.abort(error);
				throw error;
			}
			finally {
				localAbort.abort();
				socket.destroy();
			}
		})().catch((error: unknown) => {
			console.log(error);
		});
	});

	(async () => {
		// Close the listener on abort
		try {
			while (true) {
				const listener = new ChangeListener(abort.changeRoot);

				try {
					if (abort.aborted) {
						break;
					}

					// Wait for the abort state to change
					await listener.changed;
				}
				finally {
					// Ensure cleanup from all change roots
					listener.change();
				}
			}
		}
		finally {
			server.close();
		}
	})().catch((error: unknown) => {
		// There's nothing in that function that can actually throw, but just for completeness's sake
		console.log(error);
	});

	// Listen only on localhost, on the specified port, or on a random port if the port is 0
	try {
		await new Promise<void>((resolve, reject) => {
			function onListening() {
				server.off('listening', onListening);
				server.off('error', onError);
				resolve();
			}
			function onError(error: unknown) {
				server.off('listening', onListening);
				server.off('error', onError);
				reject(error);
			}
			server.on('listening', onListening);
			server.on('error', onError);
			server.listen(port, '127.0.0.1');
		});
		const address = server.address();
		if (address === null || typeof address !== 'object') {
			throw new Error('Failed to retrieve bound address for listener');
		}
		return address.port;
	}
	catch (error) {
		// If listener failed to start, ensure the connection to the server is closed as well
		abort.abort(error);
		throw error;
	}
}
