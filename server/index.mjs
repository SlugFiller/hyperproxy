import DHT from 'hyperdht';
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from '@scure/bip39';
import {
	wordlist
} from '@scure/bip39/wordlists/english';
import {
	createConnection,
	createServer,
} from 'node:net';
import {
	argv,
	platform,
} from 'node:process';
import {
	DatabaseSync,
} from 'node:sqlite';
import {
	fileURLToPath,
} from 'node:url';
import {
	anyPacket,
	combineStreams,
	consumeBuffer,
	consumeUInt32LE,
	packetUInt32LE,
	readerFromNodeStream,
	runStream,
	streamPacketer,
	writerFromNodeStream,
} from './parse-utils.mjs';
import {
	RPC_LIST,
} from './rpc-commands.mjs';

const CMD_SHOW_KEY = 0;
const CMD_SHOW_SERVICES = 1;
const CMD_SHOW_REMOTES = 2;
const CMD_ADD_SERVICE = 3;
const CMD_REMOVE_SERVICE = 4;
const CMD_ADD_REMOTE = 5;
const CMD_REMOVE_REMOTE = 6;

const CMD_PATH = platform === 'win32' ? '\\\\.\\pipe\\hyperproxy-daemon' : '/tmp/hyperproxy-daemon.sock';

async function daemon() {
	const db = new DatabaseSync(fileURLToPath(new URL('config.sqlite', import.meta.url)));

	db.exec(`CREATE TABLE IF NOT EXISTS keypair(
		publicKey BLOB,
		secretKey BLOB
	)`);
	db.exec(`CREATE TABLE IF NOT EXISTS remotes(
		publicKey BLOB
	)`);
	db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS remotes__pubkey ON remotes (publicKey)`);
	db.exec(`CREATE TABLE IF NOT EXISTS services(
		port INTEGER,
		name BLOB
	)`);
	db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS services__port ON services (port)`);
	db.exec(`CREATE INDEX IF NOT EXISTS services__name ON services (LENGTH(name), name)`);

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

	server.on('connection', (socket) => {
		console.log('Connection from socket', entropyToMnemonic(socket.remotePublicKey, wordlist));
		runStream(combineStreams(
			readerFromNodeStream(socket),
			async function* (source, { signal } = {}) {
				for await (const packeter of streamPacketer(source({ signal }))) {
					switch (await consumeUInt32LE(packeter)) {
						case RPC_LIST: {
							for (const name of ['Placeholder', 'Dummy', 'Foobar']) {
								const service = Buffer.from(name);
								yield packetUInt32LE(service.byteLength);
								yield service;
							}
						}
					}
				}
			},
			writerFromNodeStream(socket)
		)).catch((e) => {
			console.log(e);
			socket.destroy(e);
		}).then(() => {
			socket.destroy();
		});
	});

	await server.listen(keyPair);

	const cmd = createServer();

	cmd.on('connection', (socket) => {
		runStream(combineStreams(
			readerFromNodeStream(socket),
			async function* (source, { signal } = {}) {
				for await (const packeter of streamPacketer(source({ signal }))) {
					switch (await consumeUInt32LE(packeter)) {
						case CMD_SHOW_KEY: {
							yield packetUInt32LE(keyPair.publicKey.byteLength);
							yield keyPair.publicKey;
							break;
						}
						case CMD_SHOW_SERVICES: {
							for (const row of db.prepare(`SELECT port, name FROM services ORDER BY port ASC`).iterate()) {
								yield packetUInt32LE(row.port);
								yield packetUInt32LE(row.name.byteLength);
								yield row.name;
							}
							break;
						}
						case CMD_SHOW_REMOTES: {
							for (const row of db.prepare(`SELECT publicKey FROM remotes ORDER BY publicKey ASC`).iterate()) {
								yield packetUInt32LE(row.publicKey.byteLength);
								yield row.publicKey;
							}
							break;
						}
						case CMD_ADD_SERVICE: {
							const port = await consumeUInt32LE(packeter);
							const name = await consumeBuffer(packeter);
							db.prepare(`REPLACE INTO services(port, name) VALUES ($port, $name)`).run({
								$port: port,
								$name: name,
							});
							break;
						}
						case CMD_REMOVE_SERVICE: {
							const port = await consumeUInt32LE(packeter);
							db.prepare(`DELETE FROM services WHERE port = $port`).run({
								$port: port,
							});
							break;
						}
						case CMD_ADD_REMOTE: {
							const publicKey = await consumeBuffer(packeter);
							db.prepare(`INSERT OR IGNORE INTO remotes(publicKey) VALUES ($publicKey)`).run({
								$publicKey: publicKey,
							});
							break;
						}
						case CMD_REMOVE_REMOTE: {
							const publicKey = await consumeBuffer(packeter);
							db.prepare(`DELETE FROM remotes WHERE publicKey = $publicKey`).run({
								$publicKey: publicKey,
							});
							break;
						}
					}
				}
			},
			writerFromNodeStream(socket)
		)).catch((e) => {
			socket.destroy(e);
		}).then(() => {
			socket.destroy();
		});
	});

	cmd.listen(CMD_PATH);
}

async function runCommand(writer, reader) {
	const socket = createConnection(CMD_PATH);
	const controller = new AbortController();
	try {
		const writeTask = runStream(combineStreams(
			writer,
			writerFromNodeStream(socket)
		)).catch((error) => {
			controller.abort(error);
		});
		for await (const packeter of streamPacketer(readerFromNodeStream(socket)(null, { signal: controller.signal }))) {
			await reader(packeter);
		}
		await writeTask;
	}
	finally {
		socket.destroy();
	}

}

function printKey(publicKey) {
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

	console.log([...chunk(entropyToMnemonic(publicKey, wordlist).split(' ').map(x => x.padEnd(8)), 4).map(x => x.join(' '))].join('\n'));
}

async function app() {
	if (argv[2] === 'daemon') {
		return await daemon();
	}

	if (argv[2] === 'show' && argv[3] === 'key') {
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_SHOW_KEY);
		}, async(packeter) => {
			printKey(await consumeBuffer(packeter));
		});
	}

	if (argv[2] === 'show' && argv[3] === 'services') {
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_SHOW_SERVICES);
		}, async(packeter) => {
			while (await anyPacket(packeter)) {
				const port = await consumeUInt32LE(packeter);
				const name = (await consumeBuffer(packeter)).toString();
				console.log(`${ port.toString().padStart(8) } ${name}`);
			}
		});
	}

	if (argv[2] === 'show' && argv[3] === 'allowed') {
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_SHOW_REMOTES);
		}, async(packeter) => {
			if (await anyPacket(packeter)) {
				printKey(await consumeBuffer(packeter));
			}
			while (await anyPacket(packeter)) {
				console.log();
				printKey(await consumeBuffer(packeter));
			}
		});
	}

	if (argv[2] === 'add') {
		if (!/^[0-9]+$/.test(argv[3])) {
			console.log('Please specify a port');
			console.log();
			console.log('Usage:');
			console.log('  add <port> <name>');
			return;
		}
		const name = argv.slice(4).join(' ').trim();
		if (name === '') {
			console.log('Please specify a name');
			console.log();
			console.log('Usage:');
			console.log('  add <port> <name>');
			return;
		}
		const port = parseInt(argv[3]);
		const nameBuf = Buffer.from(name);
		if (port < 1 || port > 65535) {
			console.log('Port must be between 1 and 65535');
			return;
		}
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_ADD_SERVICE);
			yield packetUInt32LE(port);
			yield packetUInt32LE(nameBuf.byteLength);
			yield nameBuf;
		}, async() => {
		});
	}

	if (argv[2] === 'remove') {
		if (!/^[0-9]+$/.test(argv[3])) {
			console.log('Please specify a port');
			console.log();
			console.log('Usage:');
			console.log('  add <port> <name>');
			return;
		}
		const port = parseInt(argv[3]);
		if (port < 1 || port > 65535) {
			console.log('Port must be between 1 and 65535');
			return;
		}
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_REMOVE_SERVICE);
			yield packetUInt32LE(port);
		}, async() => {
		});
	}

	if (argv[2] === 'allow') {
		const pubkey_phrase = argv.slice(3).join(' ').trim();
		if (pubkey_phrase === '') {
			console.log('Please specify a public key as a BIP39 mnemonic phrase');
			console.log();
			console.log('Usage:');
			console.log('  allow <public key>');
			return;
		}
		let pubkey;
		try {
			pubkey = mnemonicToEntropy(pubkey_phrase, wordlist);
		}
		catch (e) {
			console.log('Not a valid BIP39 phrase:');
			console.log(pubkey_phrase);
			console.log(e.message);
		}
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_ADD_REMOTE);
			yield packetUInt32LE(pubkey.byteLength);
			yield pubkey;
		}, async() => {
		});
	}

	if (argv[2] === 'deny') {
		const pubkey_phrase = argv.slice(3).join(' ').trim();
		if (pubkey_phrase === '') {
			console.log('Please specify a public key as a BIP39 mnemonic phrase');
			console.log();
			console.log('Usage:');
			console.log('  allow <public key>');
			return;
		}
		let pubkey;
		try {
			pubkey = mnemonicToEntropy(pubkey_phrase, wordlist);
		}
		catch (e) {
			console.log('Not a valid BIP39 phrase:');
			console.log(pubkey_phrase);
			console.log(e.message);
		}
		return await runCommand(async function* () {
			yield packetUInt32LE(CMD_REMOVE_REMOTE);
			yield packetUInt32LE(pubkey.byteLength);
			yield pubkey;
		}, async() => {
		});
	}

	console.log('Commands:');
	console.log('  daemon              Start the daemon and listen for connections and commands');
	console.log('  show key            Show the public key');
	console.log('  show services       Show the list of available services');
	console.log('  show allowed        Show the list of allowed remote public keys');
	console.log('  add <port> <name>   Add the specified service to the list');
	console.log('  remove <port>       Remove the specified service from the service list');
	console.log('  allow <public key>  Add the specified public key to the allowed list');
	console.log('  deny <public key>   Remove the specified public key from the allowed list');
}

await app();
