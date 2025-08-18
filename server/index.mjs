import DHT from 'hyperdht';
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from 'bip39';
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

await server.listen(keyPair)

server.on('connection', function (socket) {
	console.log('Connection from socket', entropyToMnemonic(socket.remotePublicKey));
	pipeline(
		socket,
		async function* (source, { signal }) {
		},
		socket
	);
});

console.log(entropyToMnemonic(keyPair.publicKey));
