// /* global Bare, BareKit */

import '../src/polyfills.mjs';
import DHT from 'hyperdht';
import {
	RPC_KEYGEN,
	RPC_LIST,
} from './rpc-commands.mjs';
import {
	StreamSplitter,
	combineStreams,
	consumeUInt32LE,
	packetUInt32LE,
	readerFromNodeStream,
	runStream,
	streamPacketer,
	writerFromNodeStream,
} from '../src/parse-utils.mjs';

const splitter = new StreamSplitter(async function* (stream, { signal }) {
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
