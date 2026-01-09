import type {
	Duplex,
} from 'bare-stream';
import DHT from 'hyperdht';
import {
	addOrGetProxy,
	getProxy,
	removeProxy,
	removeAllProxies,
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
	createClosedReadPin,
	createPipe,
	receiveStop,
	receiveValue,
	sendFinish,
	sendValue,
} from '../src/pin-stream/pin-stream.ts';
import type {
	PipeReadPin,
	PipeWritePin,
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

declare global {
	const BareKit: {
		IPC: Duplex;
	}
}

const node = new DHT();

async function handleRequest(readPin: PipeReadPin<Uint8Array>, writePin: PipeWritePin<Uint8Array>, options?: {
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
			const { readPin: commandReadPin, writePin: commandWritePin } = createPipe<Uint8Array>();

			// Connect to server
			const socket = node.connect(request.serverKey, {
				keyPair: {
					publicKey: request.publicKey,
					secretKey: request.secretKey,
				},
			});
			socket.on('error', (error: unknown) => {
				// The default behavior for `EventEmitter` is to crash the VM if an error
				// is emitted without a handler. This handler pre-emptively prevents this
				console.log('Request socket error', error);
			});

			try {
				await runProcesses(async (processes, optionsProc) => {
					await sendValue(processes, async (optionsSub) => {
						// Stream the result directly to the write pin
						await processReadable(socket, writePin, optionsSub);
					}, optionsProc);

					await sendValue(processes, async (optionsSub) => {
						await processWritable(commandReadPin, socket, optionsSub);
					}, optionsProc);

					await sendFinish(processes, optionsProc);

					// Send the request packet
					await sendServerRequest(commandWritePin, {
						type: 'list',
					}, optionsProc);

					await sendFinish(commandWritePin, optionsProc);
				}, options);
			}
			finally {
				socket.destroy();
			}
			break;
		}

		case 'proxy': {
			await sendRPCResponse(writePin, {
				type: 'proxy',
				port: await addOrGetProxy(node, {
					publicKey: request.publicKey,
					secretKey: request.secretKey,
				}, request.serverKey, request.serviceName, request.port),
			}, options);
			await sendFinish(writePin, options);
			break;
		}

		case 'unproxy': {
			removeProxy(request.serverKey, request.serviceName);
			await sendFinish(writePin, options);
			break;
		}

		case 'is_proxy': {
			await sendRPCResponse(writePin, {
				type: 'is_proxy',
				port: getProxy(request.serverKey, request.serviceName),
			});
			await sendFinish(writePin, options);
			break;
		}

		case 'unproxy_all': {
			removeAllProxies(request.serverKey);
			await sendFinish(writePin, options);
			break;
		}
	}
}

try {
	await runProcesses(async (processes, optionsProc) => {
		const { readPin, writePin: joinedOutput } = createPipe<Uint8Array>();
		const { readPin: joinedInput, writePin } = createPipe<Uint8Array>();
		const { readPin: remoteStreamsReadPin, writePin: remoteStreamsWritePin } = createPipe<StreamSplitterStream>();

		await sendValue(processes, async (options) => {
			await processReadable(BareKit.IPC, writePin, options);
		}, optionsProc);

		await sendValue(processes, async (options) => {
			await processWritable(readPin, BareKit.IPC, options);
		}, optionsProc);

		await sendValue(processes, async (options) => {
			await streamSplitter(joinedInput, joinedOutput, createClosedReadPin<StreamSplitterStream>(), remoteStreamsWritePin, options);
		}, optionsProc);

		while (true) {
			const result = await receiveValue(remoteStreamsReadPin, optionsProc);
			if (result.done) {
				break;
			}

			const {
				readPin: requestReadPin,
				writePin: requestWritePin,
				localAbort,
				remoteAbort,
			} = result.value;

			await sendValue(processes, async () => {
				try {
					await runProcesses(async (unused, optionsConn) => {
						// Not using processes spawning context
						// Only running this to combine aborts
						await sendFinish(unused, optionsConn);

						await handleRequest(requestReadPin, requestWritePin, optionsConn);
					}, {
						abort: optionsProc?.abort,
						aborts: [
							// Also abort on remote abort
							remoteAbort,
						],
					});
				}
				catch (error) {
					console.log(error);
					localAbort?.abort(error);
				}
				finally {
					localAbort?.abort();
				}
			}, optionsProc);
		}

		await sendFinish(processes, optionsProc);
	});
}
catch (e) {
	console.log(e);
}
finally {
	BareKit.IPC.destroy();
	await node.destroy();
}
