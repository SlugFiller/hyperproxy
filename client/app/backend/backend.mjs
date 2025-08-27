// /* global Bare, BareKit */

import RPC from 'bare-rpc';
import DHT from 'hyperdht';
import {
	RPC_KEYGEN,
} from './rpc-commands.mjs'

const rpc = new RPC(BareKit.IPC, async (req) => {
	try {
		switch (req.command) {
			case RPC_KEYGEN: {
				const key = DHT.keyPair();
				const pubKeyLen = Buffer.alloc(4);
				pubKeyLen.writeUInt32LE(key.publicKey.length);
				req.reply(Buffer.concat([pubKeyLen, key.publicKey, key.secretKey]));
			}
		}
	}
	finally {
		if (!req.sent) {
			throw new Error('Command not handled');
		}
	}
});
