/**
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
import b4a from 'b4a';
import {
	sendServerRequest,
} from './protocol.ts';
import {
	Abort,
} from '../src/pin-stream/abort.ts';
import {
	ChangeListener,
} from '../src/pin-stream/change.ts';
import {
	createClosedWritePin,
	createPipe,
	sendFinish,
	sendValue,
} from '../src/pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from '../src/pin-stream/pin-stream-compat.ts';
import {
	runProcesses,
} from '../src/pin-stream/processes.ts';
import {
	streamSplitter,
} from '../src/pin-stream/stream-splitter.ts';
import type {
	StreamSplitterStream,
} from '../src/pin-stream/stream-splitter.ts';
import {
	waitTimeout,
} from '../src/pin-stream/timeout.ts';
import {
	Unrace,
} from '../src/pin-stream/unrace.ts';

const activeProxies = new Map<string, Map<string, {
	port: number,
	abort: Abort,
}>>();

export async function addOrGetProxy(node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverKey: Uint8Array, serviceName: Uint8Array, port: number): Promise<number> {
	const server64 = b4a.toString(serverKey, 'base64');
	const service64 = b4a.toString(serviceName, 'base64');
	if (!activeProxies.has(server64)) {
		activeProxies.set(server64, new Map());
	}
	const services = activeProxies.get(server64)!;
	const prev = services.get(service64);
	if (prev) {
		return prev.port;
	}
	try {
		const created = await createProxy(node, keyPair, serverKey, serviceName, port);
		services.set(service64, created);
		return created.port;
	}
	finally {
		if (services.size < 1) {
			activeProxies.delete(server64)
		}
	}
}

export function getProxy(serverKey: Uint8Array, serviceName: Uint8Array): number {
	const server64 = b4a.toString(serverKey, 'base64');
	const service64 = b4a.toString(serviceName, 'base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return 0;
	}
	const proxy = services.get(service64);
	if (!proxy) {
		return 0;
	}
	return proxy.port;
}

export function removeProxy(serverKey: Uint8Array, serviceName: Uint8Array): void {
	const server64 = b4a.toString(serverKey, 'base64');
	const service64 = b4a.toString(serviceName, 'base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return;
	}
	const proxy = services.get(service64);
	if (!proxy) {
		return;
	}
	services.delete(service64);
	if (services.size < 1) {
		activeProxies.delete(server64)
	}
	proxy.abort.abort();
}

export function removeAllProxies(serverKey: Uint8Array): void {
	const server64 = b4a.toString(serverKey, 'base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return;
	}
	for (const { abort } of services.values()) {
		abort.abort();
	}
	activeProxies.delete(server64)
}

async function createProxy(node: DHT, keyPair: {
	publicKey: Uint8Array,
	secretKey: Uint8Array,
}, serverKey: Uint8Array, serviceName: Uint8Array, port: number): Promise<{
	port: number,
	abort: Abort,
}> {
	const { readPin: localStreamsReadPin, writePin: localStreamsWritePin } = createPipe<StreamSplitterStream>();
	const localStreamsUnrace = new Unrace();
	const abort = new Abort();

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
		return {
			port: server.address().port,
			abort,
		};
	}
	catch (error) {
		// If listener failed to start, ensure the connection to the server is closed as well
		abort.abort(error);
		throw error;
	}
}
