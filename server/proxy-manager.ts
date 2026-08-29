/*
 * SPDX-License-Identifier: 0BSD
 *
 * BSD Zero Clause License
 *
 * Permission to use, copy, modify, and/or distribute this software for
 * any purpose with or without fee is hereby granted.
 *
 * THE SOFTWARE IS PROVIDED “AS IS” AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE
 * FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

import type DHT from 'hyperdht';
import {
	createServer,
} from 'node:net';
import {
	Abort,
	anyAbort,
} from './pin-stream/abort.ts';
import {
	Eventual,
} from './pin-stream/eventual.ts';
import {
	createClosedWritePin,
	createPipe,
	sendValue,
} from './pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from './pin-stream/pin-stream-compat.ts';
import {
	Processes,
} from './pin-stream/processes.ts';
import {
	type StreamSplitterStream,
	streamSplitter,
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

export class ProxyManager {
	#activeProxiesByServer = new Map<string, Set<ActiveProxyImpl>>();
	#activeProxiesByPort = new Map<number, ActiveProxyImpl>();
	#processes: Processes;

	constructor(options: {
		abort?: Abort,
	} = {}) {
		this.#processes = new Processes({ abort: options.abort });
	}

	async addProxy(node: DHT, keyPair: {
		publicKey: Uint8Array,
		secretKey: Uint8Array,
	}, serverName: string, serverKey: Uint8Array, serviceName: Uint8Array, port: number): Promise<number> {
		const errorAbort = new Abort();
		const result = new Eventual<number>();
		this.#processes.run(async () => {
			try {
				await using processesProxy = new Processes({ abort: this.#processes.abort })
				const proxy: ActiveProxyImpl = {
					port,
					serverName,
					serviceName,
					abort: new Abort(),
				};
				if (port !== 0 && this.#activeProxiesByPort.has(port)) {
					throw new Error(`Port ${ port } already in use by another proxy`);
				}

				// Run proxy
				const finalPort = new Eventual<number>();
				processesProxy.run(async () => {
					await runProxy(finalPort, node, keyPair, serverKey, serviceName, port, processesProxy.abort);
				});
				proxy.port = await finalPort.getValue({ abort: processesProxy.abort });

				// Add proxy to active list
				if (this.#activeProxiesByPort.has(proxy.port)) {
					throw new Error(`Port ${ proxy.port } already in use by another proxy`);
				}
				this.#activeProxiesByPort.set(proxy.port, proxy)
				if (!this.#activeProxiesByServer.has(serverName)) {
					this.#activeProxiesByServer.set(serverName, new Set());
				}
				this.#activeProxiesByServer.get(serverName)!.add(proxy);

				result.setValue(proxy.port);

				await anyAbort(proxy.abort, processesProxy.abort);
			}
			catch (error) {
				errorAbort.abort(error);
			}
		});
		return await result.getValue({ abort: errorAbort });
	}

	getActiveProxies(): ActiveProxy[] {
		return [...this.#activeProxiesByPort.values()].sort((a, b) => (a.port - b.port));
	}

	removeProxyByPort(port: number): void {
		const proxy = this.#activeProxiesByPort.get(port);
		if (!proxy) {
			return;
		}
		this.#activeProxiesByPort.delete(port);
		this.#removeProxy(proxy);
	}

	removeProxiesByServer(serverName: string): void {
		const toRemove = this.#activeProxiesByServer.get(serverName);
		if (!toRemove) {
			return;
		}
		this.#activeProxiesByServer.delete(serverName);
		for (const proxy of toRemove) {
			this.#removeProxy(proxy);
		}
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await using _ = this.#processes;
	}


	#removeProxy(proxy: ActiveProxyImpl): void {
		proxy.abort.abort();
		const serverProxies = this.#activeProxiesByServer.get(proxy.serverName);
		if (serverProxies) {
			serverProxies.delete(proxy);
			if (serverProxies.size < 1) {
				this.#activeProxiesByServer.delete(proxy.serverName);
			}
		}
		if (this.#activeProxiesByPort.get(proxy.port) === proxy) {
			this.#activeProxiesByPort.delete(proxy.port);
		}
	}
}

async function runProxy(finalPort: Eventual<number>, node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverKey: Uint8Array, serviceName: Uint8Array, port: number, abort: Abort): Promise<void> {
	const localStreams = createPipe<StreamSplitterStream>();
	const localStreamsUnrace = new Unrace();
	await using processesProxy = new Processes({ abort });

	async function retry() {
		try {
			using remoteStack = new DisposableStack();
			const remote = remoteStack.adopt(node.connect(serverKey, { keyPair }), (r) => {
				r.destroy();
			});
			remote.on('error', (error: unknown) => {
				// The default behavior for `EventEmitter` is to crash the VM if an error
				// is emitted without a handler. This handler pre-emptively prevents this
				console.log('Proxy socket error', error);
			});

			// Manage connection to server
			await using processes = new Processes({ abort: processesProxy.abort });

			const { writePin: joinedOutput, readPin: socketInput } = createPipe<Uint8Array>();
			const { writePin: socketOutput, readPin: joinedInput } = createPipe<Uint8Array>();

			processes.run(async () => {
				await processReadable(remote, socketOutput, { abort: processes.abort });
			});

			processes.run(async () => {
				await processWritable(socketInput, remote, { abort: processes.abort });
			});

			processes.run(async () => {
				// First send request
				await sendServerRequest(joinedOutput, {
					type: 'proxy',
					serviceName,
				}, { abort: processes.abort });

				// Then treat the rest of the stream as part of the stream splitter
				await streamSplitter(joinedInput, joinedOutput, localStreams.readPin, createClosedWritePin<StreamSplitterStream>(), { abort: processes.abort });
			});

			await processes.finish();
		}
		catch (error) {
			if (!processesProxy.abort.aborted) {
				// Unexpected disconnection is logged here
				console.log(error);
			}
		}
		finally {
			if (processesProxy.abort.aborted) {
				return;
			}

			// Connection failed or unexpectedly disconnected
			// Wait 5 seconds
			await waitTimeout(5000, {
				abort: processesProxy.abort,
			});
			// Spawn another connection
			processesProxy.run(retry);
		}
	}
	// Spawn connection
	processesProxy.run(retry);

	// Listen for incoming connections
	await using server = createServer();
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
		processesProxy.run(async () => {
			const localAbort = new Abort();
			try {
				using socketStack = new DisposableStack();
				socketStack.adopt(socket, (r) => {
					r.destroy();
				});
				await using processes = new Processes({ abort });
				const {
					abort: remoteAbort,
				} = processes;

				const { writePin: socketOutput, readPin } = createPipe<Uint8Array>();
				const { writePin, readPin: socketInput } = createPipe<Uint8Array>();

				processes.run(async () => {
					await processReadable(socket, socketOutput, { abort: remoteAbort });
				});

				processes.run(async () => {
					await processWritable(socketInput, socket, { abort: remoteAbort });
				});

				{
					// Send this socket as a new local connection to the stream splitter
					using _ = await localStreamsUnrace.run(undefined, { abort: remoteAbort });
					await sendValue(localStreams.writePin, {
						readPin,
						writePin,
						localAbort,
						remoteAbort,
					}, { abort: remoteAbort });
				}

				await processes.finish();
			}
			catch (error) {
				localAbort.abort(error);
				console.log(error);
			}
			finally {
				localAbort.abort();
			}
		});
	});

	// Listen only on localhost, on the specified port, or on a random port if the port is 0
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
	finalPort.setValue(address.port);
	await anyAbort(processesProxy.abort);
}
