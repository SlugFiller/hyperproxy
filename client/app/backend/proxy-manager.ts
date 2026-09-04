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
} from 'bare-tcp';
import {
	sendServerRequest,
} from './protocol.ts';
import {
	Abort,
	anyAbort,
} from '../src/pin-stream/abort.ts';
import {
	Eventual,
} from '../src/pin-stream/eventual.ts';
import {
	createClosedWritePin,
	createPipe,
	sendValue,
} from '../src/pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from '../src/pin-stream/pin-stream-compat.ts';
import {
	Processes,
} from '../src/pin-stream/processes.ts';
import {
	type StreamSplitterStream,
	streamSplitter,
} from '../src/pin-stream/stream-splitter.ts';
import {
	waitTimeout,
} from '../src/pin-stream/timeout.ts';
import {
	Unrace,
} from '../src/pin-stream/unrace.ts';

export class ProxyManager {
	#activeProxies = new Map<string, Map<string, {
		port: Eventual<number>,
		abort: Abort,
		errorAbort: Abort,
	}>>();
	#processes: Processes;

	constructor(options: {
		abort?: Abort,
	} = {}) {
		this.#processes = new Processes({ abort: options.abort });
	}

	async addOrGetProxy(node: DHT, keyPair: {
		publicKey: Uint8Array,
		secretKey: Uint8Array,
	}, serverKey: Uint8Array, serviceName: Uint8Array, port: number, abort?: Abort): Promise<number> {
		const server64 = serverKey.toBase64();
		const service64 = serviceName.toBase64();
		if (!this.#activeProxies.has(server64)) {
			this.#activeProxies.set(server64, new Map());
		}
		const services = this.#activeProxies.get(server64)!;
		const prev = services.get(service64);
		if (prev) {
			await using processesAbort = new Processes({ aborts: [prev.errorAbort, abort] });
			return await prev.port.getValue({ abort: processesAbort.abort });
		}
		const proxy = {
			port: new Eventual<number>(),
			abort: new Abort(),
			errorAbort: new Abort(),
		};
		// Pre-emptively add proxy to active list to prevent `services` from being removed
		services.set(service64, proxy);
		this.#processes.run(async () => {
			try {
				await using processesProxy = new Processes({ abort: this.#processes.abort })

				// Run proxy
				processesProxy.run(async () => {
					await runProxy(proxy.port, node, keyPair, serverKey, serviceName, port, processesProxy.abort);
				});

				{
					// Validate that getting a port succeeded
					await using processesAbort = new Processes({ aborts: [processesProxy.abort, abort] });
					await proxy.port.getValue({ abort: processesAbort.abort });
				}

				await anyAbort(proxy.abort, processesProxy.abort);
			}
			catch (error) {
				proxy.errorAbort.abort(error);
			}
			finally {
				if (services.get(service64) === proxy) {
					services.delete(service64)
				}
				if (services.size < 1 && this.#activeProxies.get(server64) === services) {
					this.#activeProxies.delete(server64)
				}
			}
		});
		await using processesAbort = new Processes({ aborts: [proxy.errorAbort, abort] });
		return await proxy.port.getValue({ abort: processesAbort.abort });
	}

	async getProxy(serverKey: Uint8Array, serviceName: Uint8Array, abort?: Abort): Promise<number> {
		const server64 = serverKey.toBase64();
		const service64 = serviceName.toBase64();
		const services = this.#activeProxies.get(server64)
		if (!services) {
			return 0;
		}
		const proxy = services.get(service64);
		if (!proxy) {
			return 0;
		}
		await using processesAbort = new Processes({ aborts: [proxy.errorAbort, abort] });
		return await proxy.port.getValue({ abort: processesAbort.abort });
	}

	removeProxy(serverKey: Uint8Array, serviceName: Uint8Array): void {
		const server64 = serverKey.toBase64();
		const service64 = serviceName.toBase64();
		const services = this.#activeProxies.get(server64)
		if (!services) {
			return;
		}
		const proxy = services.get(service64);
		if (!proxy) {
			return;
		}
		services.delete(service64);
		if (services.size < 1) {
			this.#activeProxies.delete(server64)
		}
		proxy.abort.abort();
	}

	removeAllProxies(serverKey: Uint8Array): void {
		const server64 = serverKey.toBase64();
		const services = this.#activeProxies.get(server64)
		if (!services) {
			return;
		}
		for (const { abort } of services.values()) {
			abort.abort();
		}
		this.#activeProxies.delete(server64)
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await using _ = this.#processes;
	}
}

async function runProxy(finalPort: Eventual<number>, node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverKey: Uint8Array, serviceName: Uint8Array, port: number, abort: Abort): Promise<void> {
	const localStreams = createPipe<StreamSplitterStream>();
	const localStreamsUnrace = new Unrace();
	await using processesProxy = new Processes({ abort });
	const remoteReady = new Abort();

	async function retry() {
		try {
			const remoteError = new Abort();
			const remoteConnect = new Abort();
			using remoteStack = new DisposableStack();
			const remote = remoteStack.adopt(node.connect(serverKey, { keyPair }), (r) => {
				r.destroy();
			});
			remote.on('error', (error: unknown) => {
				// The default behavior for `EventEmitter` is to crash the VM if an error
				// is emitted without a handler. This handler pre-emptively prevents this
				console.log('Proxy socket error', error);
				remoteError.abort(error);
			});
			remote.once('connect', () => {
				remoteConnect.abort();
			});

			await anyAbort(remoteConnect, remoteError, processesProxy.abort);
			if (processesProxy.abort.aborted) {
				throw processesProxy.abort.reason;
			}
			if (remoteError.aborted) {
				throw remoteError.reason;
			}

			remoteReady.abort();

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
			if (!remoteReady.aborted) {
				// Failed on first attempt, do not retry
				throw new Error('Failed to connect to proxy');
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

	// Wait for the proxy to connect at least once
	// It may disconnect and automatically reconnect later
	await anyAbort(remoteReady, processesProxy.abort);
	if (processesProxy.abort.aborted) {
		throw processesProxy.abort.reason;
	}

	// Listen for incoming connections
	const serverError = new Abort();
	const serverListening = new Abort();
	using serverStack = new DisposableStack();
	const server = serverStack.adopt(createServer(), (r) => {
		r.close();
	});
	server.on('error', (error: unknown) => {
		// The default behavior for `EventEmitter` is to crash the VM if an error
		// is emitted without a handler. This handler pre-emptively prevents this
		console.log('Listener error', error);
		serverError.abort(error);
	});
	server.once('listening', () => {
		serverListening.abort();
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
				await using processes = new Processes({ abort: processesProxy.abort });
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
	server.listen(port, '127.0.0.1');
	await anyAbort(serverListening, serverError, processesProxy.abort);
	if (processesProxy.abort.aborted) {
		throw processesProxy.abort.reason;
	}
	if (serverError.aborted) {
		throw serverError.reason;
	}
	const address = server.address();
	if (address === null || typeof address !== 'object') {
		throw new Error('Failed to retrieve bound address for listener');
	}
	finalPort.setValue(address.port);
	await anyAbort(processesProxy.abort);
}
