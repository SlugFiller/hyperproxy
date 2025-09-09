// /* global Bare, BareKit */

import '../src/polyfills.mjs';
import DHT from 'hyperdht';
import {
	addOrGetProxy,
	getProxy,
	removeProxy,
	removeAllProxies,
} from './proxy-manager.mjs';
import {
	RPC_KEYGEN,
	RPC_LIST,
	RPC_PROXY,
	RPC_UNPROXY,
	RPC_IS_PROXY,
	RPC_UNPROXY_ALL,
} from './rpc-commands.mjs';
import {
	StreamSplitter,
	combineStreams,
	consumeBuffer,
	consumeUInt32LE,
	packetUInt32LE,
	readerFromNodeStream,
	runStream,
	streamPacketer,
	writerFromNodeStream,
} from '../src/parse-utils.mjs';

const node = new DHT();

const splitter = new StreamSplitter(async function* (stream, { signal } = {}) {
	for await (const packeter of streamPacketer(stream({ signal }))) {
		switch (await consumeUInt32LE(packeter)) {
			case RPC_KEYGEN: {
				const key = DHT.keyPair();
				yield packetUInt32LE(key.publicKey.byteLength);
				yield key.publicKey;
				yield packetUInt32LE(key.secretKey.byteLength);
				yield key.secretKey;
				break;
			}
			case RPC_LIST: {
				const publicKey = await consumeBuffer(packeter);
				const secretKey = await consumeBuffer(packeter);
				const serverKey = await consumeBuffer(packeter);
				// Connect to server
				const socket = node.connect(serverKey, {
					keyPair: {
						publicKey,
						secretKey,
					},
				});
				const controller = new AbortController();
				try {
					// Send the request packet
					runStream(combineStreams(
						async function* () {
							yield packetUInt32LE(RPC_LIST);
						},
						writerFromNodeStream(socket)
					), { signal }).catch((error) => {
						controller.abort(error);
					});
					// Read the results
					yield* readerFromNodeStream(socket)(null, { signal: signal ? AbortSignal.any([ signal, controller.signal ]) : controller.signal });
				}
				catch (e) {
					console.log(e);
				}
				finally {
					socket.destroy();
				}
				break;
			}
			case RPC_PROXY: {
				const publicKey = await consumeBuffer(packeter);
				const secretKey = await consumeBuffer(packeter);
				const serverKey = await consumeBuffer(packeter);
				const serviceName = await consumeBuffer(packeter);
				yield packetUInt32LE(await addOrGetProxy(node, {
					publicKey,
					secretKey,
				}, serverKey, serviceName));
				break;
			}
			case RPC_UNPROXY: {
				const serverKey = await consumeBuffer(packeter);
				const serviceName = await consumeBuffer(packeter);
				removeProxy(serverKey, serviceName);
				break;
			}
			case RPC_IS_PROXY: {
				const serverKey = await consumeBuffer(packeter);
				const serviceName = await consumeBuffer(packeter);
				yield packetUInt32LE(getProxy(serverKey, serviceName));
				break;
			}
			case RPC_UNPROXY_ALL: {
				const serverKey = await consumeBuffer(packeter);
				removeAllProxies(serverKey);
				break;
			}
		}
	}
});

try {
	await runStream(combineStreams(
		readerFromNodeStream(BareKit.IPC),
		splitter.split,
		writerFromNodeStream(BareKit.IPC)
	));
}
catch (e) {
	console.log(e);
}
finally {
	BareKit.IPC.destroy();
}

await node.destroy();
