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

import {
	type Duplex,
} from 'bare-stream';
import DHT from 'hyperdht';
import {
	ProxyManager,
} from './proxy-manager.ts';
import {
	receiveRPCRequest,
	sendRPCResponse,
	sendServerRequest,
} from './protocol.ts';
import {
	Abort,
} from '../src/pin-stream/abort.ts';
import {
	toAsyncIterable,
} from '../src/pin-stream/iterable.ts';
import {
	type PipeReadPin,
	type PipeWritePin,
	createClosedReadPin,
	createPipe,
	receiveStop,
	sendFinish,
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

declare global {
	const BareKit: {
		IPC: Duplex;
	}
}

using kitStack = new DisposableStack();
kitStack.adopt(BareKit.IPC, (r) => {
	r.destroy();
})

await using nodeStack = new AsyncDisposableStack();
const node = nodeStack.adopt(new DHT(), async (r) => {
	await r.destroy();
});

async function handleRequest(readPin: PipeReadPin<Uint8Array>, writePin: PipeWritePin<Uint8Array>, proxyManager: ProxyManager, options?: {
	abort?: Abort,
}): Promise<void> {
	const request = await receiveRPCRequest(readPin, options);
	// One request per stream
	await receiveStop(readPin, options);
	switch (request.type) {
		case 'keygen': {
			const key = DHT.keyPair();
			await sendRPCResponse(writePin, {
				type: 'keygen',
				publicKey: key.publicKey,
				secretKey: key.secretKey,
			}, options);
			await sendFinish(writePin, options);
			break;
		}

		case 'list': {
			const { writePin: commandWritePin, readPin: socketInput } = createPipe<Uint8Array>();
			// Stream the result directly to the write pin
			const socketOutput = writePin;

			// Connect to server
			using socketStack = new DisposableStack();
			const socket = socketStack.adopt(node.connect(request.serverKey, {
				keyPair: {
					publicKey: request.publicKey,
					secretKey: request.secretKey,
				},
			}), (r) => {
				r.destroy();
			});
			socket.on('error', (error: unknown) => {
				// The default behavior for `EventEmitter` is to crash the VM if an error
				// is emitted without a handler. This handler pre-emptively prevents this
				console.log('Request socket error', error);
			});

			await using processes = new Processes(options);

			processes.run(async () => {
				await processReadable(socket, socketOutput, { abort: processes.abort });
			});

			processes.run(async () => {
				await processWritable(socketInput, socket, { abort: processes.abort });
			});

			// Send the request packet
			await sendServerRequest(commandWritePin, {
				type: 'list',
			}, { abort: processes.abort });

			await sendFinish(commandWritePin, { abort: processes.abort });

			await processes.finish();
			break;
		}

		case 'proxy': {
			await sendRPCResponse(writePin, {
				type: 'proxy',
				port: await proxyManager.addOrGetProxy(node, {
					publicKey: request.publicKey,
					secretKey: request.secretKey,
				}, request.serverKey, request.serviceName, request.port),
			}, options);
			await sendFinish(writePin, options);
			break;
		}

		case 'unproxy': {
			proxyManager.removeProxy(request.serverKey, request.serviceName);
			await sendFinish(writePin, options);
			break;
		}

		case 'is_proxy': {
			await sendRPCResponse(writePin, {
				type: 'is_proxy',
				port: proxyManager.getProxy(request.serverKey, request.serviceName),
			});
			await sendFinish(writePin, options);
			break;
		}

		case 'unproxy_all': {
			proxyManager.removeAllProxies(request.serverKey);
			await sendFinish(writePin, options);
			break;
		}
	}
}

try {
	await using proxyManager = new ProxyManager();
	await using processes = new Processes();

	const { writePin: joinedOutput, readPin } = createPipe<Uint8Array>();
	const { writePin, readPin: joinedInput } = createPipe<Uint8Array>();
	const remoteStreams = createPipe<StreamSplitterStream>();

	processes.run(async () => {
		await processReadable(BareKit.IPC, writePin, { abort: processes.abort });
	});

	processes.run(async () => {
		await processWritable(readPin, BareKit.IPC, { abort: processes.abort });
	});

	processes.run(async () => {
		await streamSplitter(joinedInput, joinedOutput, createClosedReadPin<StreamSplitterStream>(), remoteStreams.writePin, { abort: processes.abort });
	});

	for await (const {
		readPin: requestReadPin,
		writePin: requestWritePin,
		localAbort,
		remoteAbort,
	} of toAsyncIterable(remoteStreams.readPin, { abort: processes.abort })) {
		processes.run(async () => {
			try {
				await using processesConn = new Processes({
					aborts: [
						processes.abort,
						// Also abort on remote abort
						remoteAbort,
					],
				});

				await handleRequest(requestReadPin, requestWritePin, proxyManager, { abort: processesConn.abort });
			}
			catch (error) {
				console.log(error);
				localAbort?.abort(error);
			}
			finally {
				localAbort?.abort();
			}
		});
	}

	await processes.finish();
}
catch (e) {
	console.log(e);
}
