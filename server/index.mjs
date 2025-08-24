import DHT from 'hyperdht';
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from '@scure/bip39';
import {
	wordlist
} from '@scure/bip39/wordlists/english';
import {
	DatabaseSync,
} from 'node:sqlite';
import {
	fileURLToPath,
} from 'node:url';
import {
	pipeline,
} from 'node:stream/promises';

const db = new DatabaseSync(fileURLToPath(new URL('config.sqlite', import.meta.url)));

db.exec(`CREATE TABLE IF NOT EXISTS keypair(
	publicKey BLOB,
	secretKey BLOB
)`);

const node = new DHT();

let keyPair = db.prepare(`SELECT publicKey, secretKey FROM keypair LIMIT 1`).get();
if (!keyPair) {
	keyPair = DHT.keyPair();
	db.prepare(`INSERT INTO keypair(publicKey, secretKey) SELECT $publicKey, $secretKey
		FROM (SELECT 1 t) t LEFT JOIN keypair e ON (1=1) WHERE e.ROWID IS NULL`).run({
		$publicKey: keyPair.publicKey,
		$secretKey: keyPair.secretKey,
	});
}
else {
	keyPair = {
		publicKey: Buffer.from(keyPair.publicKey),
		secretKey: Buffer.from(keyPair.secretKey),
	};
}

const server = node.createServer();

server.on('connection', function (socket) {
	console.log('Connection from socket', entropyToMnemonic(socket.remotePublicKey, wordlist));
	pipeline(
		socket,
		async function* (source, { signal }) {
		},
		socket
	);
});

await server.listen(keyPair)

function* chunk(iter, len) {
	let out = [];
	for (const item of iter) {
		out.push(item);
		if (out.length >= len) {
			yield out;
			out = [];
		}
	}
	if (out.length) {
		yield out;
	}
}

console.log([...chunk(entropyToMnemonic(keyPair.publicKey, wordlist).split(' ').map(x => x.padEnd(8)), 4).map(x => x.join(' '))].join('\n'));
