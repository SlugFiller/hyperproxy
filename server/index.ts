import DHT from 'hyperdht';
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from '@scure/bip39';
import {
	wordlist,
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
import type {
	Duplex,
} from 'node:stream';
import {
	fileURLToPath,
} from 'node:url';
import {
	drainReadable,
} from './drain-readable.ts';
import {
	Abort,
} from './pin-stream/abort.ts';
import {
	createClosedReadPin,
	createPipe,
	receiveStop,
	receiveValue,
	sendFinish,
	sendValue,
} from './pin-stream/pin-stream.ts';
import type {
	PipeReadPin,
	PipeWritePin,
} from './pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from './pin-stream/pin-stream-compat.ts';
import {
	runProcesses,
} from './pin-stream/processes.ts';
import {
	streamSplitter,
} from './pin-stream/stream-splitter.ts';
import type {
	StreamSplitterStream,
} from './pin-stream/stream-splitter.ts';
import {
	receiveCmdRemoteList,
	receiveCmdRequest,
	receiveCmdResponse,
	receiveCmdServiceList,
	receiveServerRequest,
	sendCmdRemoteList,
	sendCmdRequest,
	sendCmdResponse,
	sendCmdServiceList,
	sendServerServiceList,
} from './protocol.ts';

const CMD_PATH = platform === 'win32' ? '\\\\.\\pipe\\hyperproxy-daemon' : '/tmp/hyperproxy-daemon.sock';

async function daemon(): Promise<void> {
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
	db.exec(`CREATE INDEX IF NOT EXISTS services__name_nolen ON services (name)`);

	const queryKeyPair = db.prepare(`SELECT publicKey, secretKey FROM keypair LIMIT 1`);
	const queryKeyPairStore = db.prepare(`INSERT INTO keypair(publicKey, secretKey) SELECT $publicKey, $secretKey
		FROM (SELECT 1 t) t LEFT JOIN keypair e ON (1=1) WHERE e.ROWID IS NULL`);
	const queryCheckRemote = db.prepare(`SELECT 1 FROM remotes WHERE publicKey = $publicKey`);
	const queryListServices = db.prepare(`SELECT name FROM services ORDER BY name ASC`);
	const queryCheckServiceLength = db.prepare(`SELECT 1 FROM services WHERE LENGTH(name) = $length LIMIT 1`);
	const queryCheckServicePrefix = db.prepare(`SELECT 1 FROM services WHERE LENGTH(name) = $length AND name >= $prefix AND name < $prefixPlus LIMIT 1`);
	const queryGetServiceByName = db.prepare(`SELECT port FROM services WHERE LENGTH(name) = $length AND name = $match LIMIT 1`);
	const queryGetServices = db.prepare(`SELECT port, name FROM services ORDER BY port ASC`);
	const queryGetRemotes = db.prepare(`SELECT publicKey FROM remotes ORDER BY publicKey ASC`);
	const queryAddService = db.prepare(`REPLACE INTO services(port, name) VALUES ($port, $name)`);
	const queryRemoveService = db.prepare(`DELETE FROM services WHERE port = $port`);
	const queryAddRemote = db.prepare(`INSERT OR IGNORE INTO remotes(publicKey) VALUES ($publicKey)`);
	const queryRemoveRemote = db.prepare(`DELETE FROM remotes WHERE publicKey = $publicKey`);

	const node = new DHT();

	let keyPair: {
		publicKey: Uint8Array,
		secretKey: Uint8Array,
	};
	const dbKey = queryKeyPair.get() as (undefined | {
		publicKey: Uint8Array,
		secretKey: Uint8Array,
	});
	if (!dbKey) {
		keyPair = DHT.keyPair();
		queryKeyPairStore.run({
			$publicKey: keyPair.publicKey,
			$secretKey: keyPair.secretKey,
		});
	}
	else {
		keyPair = {
			publicKey: Buffer.from(dbKey.publicKey),
			secretKey: Buffer.from(dbKey.secretKey),
		};
	}

	const server = node.createServer({
		firewall: (remotePublicKey: Uint8Array): boolean => {
			const row = queryCheckRemote.get({
				$publicKey: remotePublicKey,
			});
			return !row;
		}
	});
	server.on('error', (error: unknown) => {
		// The default behavior for `EventEmitter` is to crash the VM if an error
		// is emitted without a handler. This handler pre-emptively prevents this
		console.log('DHT server error', error);
	});

	server.on('connection', (socket: Duplex) => {
		socket.on('error', (error: unknown) => {
			// The default behavior for `EventEmitter` is to crash the VM if an error
			// is emitted without a handler. This handler pre-emptively prevents this
			console.log('DHT socket error', error);
		});
		runProcesses(async (processes, optionsProc = {}) => {
			const {
				abort,
			} = optionsProc;

			const { readPin, writePin: socketWritePin } = createPipe<Uint8Array>();
			const { readPin: socketReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await processReadable(socket, socketWritePin, options);
			}, optionsProc);

			await sendValue(processes, async (options) => {
				await processWritable(socketReadPin, socket, options);
			}, optionsProc);

			const request = await receiveServerRequest(readPin, {
				abort,
				serviceLengthChecker(length: number): Promise<void> {
					if (!queryCheckServiceLength.get({
						$length: length,
					})) {
						return Promise.reject(new Error('Service does not exist'));
					}
					return Promise.resolve();
				},
				servicePrefixChecker(length: number, prefix: Uint8Array): Promise<void> {
					const prefixPlus = Buffer.from(prefix);
					prefixPlus[prefixPlus.byteLength - 1]++;
					if (!queryCheckServicePrefix.get({
						$length: length,
						$prefix: prefix,
						$prefixPlus: prefixPlus,
					})) {
						return Promise.reject(new Error('Service does not exist'));
					}
					return Promise.resolve();
				},
			});

			switch (request.type) {
				case 'list': {
					await receiveStop(readPin, optionsProc);

					const { readPin: listReadPin, writePin: listWritePin } = createPipe<Uint8Array>();

					await sendValue(processes, async (options) => {
						await sendServerServiceList(listReadPin, writePin, options);
					}, optionsProc);

					for (const row of queryListServices.iterate()) {
						await sendValue(listWritePin, (row as {
							name: Uint8Array,
						}).name, optionsProc);
					}

					await sendFinish(listWritePin, optionsProc);

					// Due to a bug in hyperdht, this is the only way to ensure all sent
					// messages were received before destroying the socket
					await drainReadable(socket, {
						abort,
						timeout: 10000,
					});
					break;
				}

				case 'proxy': {
					const row = queryGetServiceByName.get({
						$length: request.serviceName.length,
						$match: request.serviceName,
					}) as (undefined | {
						port: number,
					});
					if (!row) {
						throw new Error('Service does not exist');
					}
					const port = row.port;

					const { readPin: remoteStreamsReadPin, writePin: remoteStreamsWritePin } = createPipe<StreamSplitterStream>();

					await sendValue(processes, async (options) => {
						await streamSplitter(readPin, writePin, createClosedReadPin<StreamSplitterStream>(), remoteStreamsWritePin, options);
					}, optionsProc);

					while (true) {
						const result = await receiveValue(remoteStreamsReadPin, optionsProc);
						if (result.done) {
							break;
						}

						const {
							readPin: connectionReadPin,
							writePin: connectionWritePin,
							localAbort,
							remoteAbort,
						} = result.value;

						await sendValue(processes, async () => {
							try {
								const proxied = createConnection(port, 'localhost');
								proxied.on('error', (error: unknown) => {
									// The default behavior for `EventEmitter` is to crash the VM if an error
									// is emitted without a handler. This handler pre-emptively prevents this
									console.log('Proxied socket error', error);
								});

								try {
									await runProcesses(async (processesConn, optionsConn) => {
										// Bridge connection
										await sendValue(processesConn, async (optionsWrite) => {
											await processReadable(proxied, connectionWritePin, optionsWrite);
										}, optionsConn);

										await sendFinish(processesConn, optionsConn)

										await processWritable(connectionReadPin, proxied, optionsConn);
									}, {
										abort: optionsProc?.abort,
										aborts: [
											// Also abort on remote abort
											remoteAbort,
										],
									});
								}
								finally {
									proxied?.destroy();
								}
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

					break;
				}
			}

			await sendFinish(processes, optionsProc);
		}).catch((error: unknown) => {
			console.log(error);
		}).then(() => {
			socket.destroy();
		}).catch((error: unknown) => {
			console.log(error);
		});
	});

	await server.listen(keyPair);

	const cmd = createServer();
	cmd.on('error', (error: unknown) => {
		// The default behavior for `EventEmitter` is to crash the VM if an error
		// is emitted without a handler. This handler pre-emptively prevents this
		console.log('Command server error', error);
	});

	cmd.on('connection', (socket) => {
		socket.on('error', (error: unknown) => {
			// The default behavior for `EventEmitter` is to crash the VM if an error
			// is emitted without a handler. This handler pre-emptively prevents this
			console.log('Command socket error', error);
		});
		runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: socketWritePin } = createPipe<Uint8Array>();
			const { readPin: socketReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await processReadable(socket, socketWritePin, options);
			}, optionsProc);

			await sendValue(processes, async (options) => {
				await processWritable(socketReadPin, socket, options);
			}, optionsProc);

			const request = await receiveCmdRequest(readPin, optionsProc);

			await receiveStop(readPin, optionsProc);

			switch (request.type) {
				case 'show_key': {
					await sendCmdResponse(writePin, {
						type: 'show_key',
						publicKey: keyPair.publicKey,
					}, optionsProc);
					await sendFinish(writePin, optionsProc);
					break;
				}

				case 'show_services': {
					const { readPin: listReadPin, writePin: listWritePin } = createPipe<{
						port: number,
						name: Uint8Array,
					}>();

					await sendValue(processes, async (options) => {
						await sendCmdServiceList(listReadPin, writePin, options);
					}, optionsProc);

					for (const row of queryGetServices.iterate()) {
						await sendValue(listWritePin, row as {
							port: number,
							name: Uint8Array,
						}, optionsProc);
					}

					await sendFinish(listWritePin, optionsProc);
					break;
				}

				case 'show_remotes': {
					const { readPin: listReadPin, writePin: listWritePin } = createPipe<Uint8Array>();

					await sendValue(processes, async (options) => {
						await sendCmdRemoteList(listReadPin, writePin, options);
					}, optionsProc);

					for (const row of queryGetRemotes.iterate()) {
						await sendValue(listWritePin, (row as {
							publicKey: Uint8Array,
						}).publicKey, optionsProc);
					}

					await sendFinish(listWritePin, optionsProc);
					break;
				}

				case 'add_service': {
					queryAddService.run({
						$port: request.port,
						$name: request.name,
					});
					await sendFinish(writePin, optionsProc);
					break;
				}

				case 'remove_service': {
					queryRemoveService.run({
						$port: request.port,
					});
					await sendFinish(writePin, optionsProc);
					break;
				}

				case 'add_remote': {
					queryAddRemote.run({
						$publicKey: request.publicKey,
					});
					await sendFinish(writePin, optionsProc);
					break;
				}

				case 'remove_remote': {
					queryRemoveRemote.run({
						$publicKey: request.publicKey,
					});
					await sendFinish(writePin, optionsProc);
					break;
				}
			}

			await sendFinish(processes, optionsProc);
		}).catch((error: unknown) => {
			console.log(error);
		}).then(() => {
			socket.destroy();
		});
	});

	cmd.listen(CMD_PATH);
}

async function runCommand(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	const socket = createConnection(CMD_PATH);

	try {
		await runProcesses(async (processes, optionsProc = {}) => {
			// Bridge connection
			await sendValue(processes, async (optionsWrite) => {
				await processReadable(socket, output, optionsWrite);
			}, optionsProc);

			await sendFinish(processes, optionsProc)

			await processWritable(input, socket, optionsProc);
		}, options);
	}
	finally {
		socket.destroy();
	}
}

function printKey(publicKey: Uint8Array): void {
	function* chunk(iter: Iterable<string>, len: number): Generator<string[], void, void> {
		let out: string[] = [];
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

async function app(): Promise<void> {
	if (argv[2] === 'daemon') {
		return await daemon();
	}

	if (argv[2] === 'show' && argv[3] === 'key') {
		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'show_key',
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// Print result
			const result = await receiveCmdResponse(readPin, 'show_key', optionsProc);
			printKey(result.publicKey);

			await receiveStop(readPin);
		});
	}

	if (argv[2] === 'show' && argv[3] === 'services') {
		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'show_services',
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// Receive list
			const { readPin: listReadPin, writePin: listWritePin } = createPipe<{
				port: number,
				name: Uint8Array,
			}>();

			await sendValue(processes, async (options) => {
				await receiveCmdServiceList(readPin, listWritePin, options);
			}, optionsProc);

			// Print result
			while (true) {
				const result = await receiveValue(listReadPin, optionsProc);
				if (result.done) {
					break;
				}
				const {
					port,
					name,
				} = result.value;
				console.log(`${ port.toString().padStart(8) } ${ Buffer.from(name).toString() }`);
			}
		});
	}

	if (argv[2] === 'show' && argv[3] === 'allowed') {
		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'show_remotes',
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// Receive list
			const { readPin: listReadPin, writePin: listWritePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await receiveCmdRemoteList(readPin, listWritePin, options);
			}, optionsProc);

			// Print result
			const first = await receiveValue(listReadPin, optionsProc);
			if (first.done) {
				return;
			}

			printKey(first.value);

			while (true) {
				const result = await receiveValue(listReadPin, optionsProc);
				if (result.done) {
					break;
				}
				console.log();
				printKey(result.value);
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

		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'add_service',
					port,
					name: nameBuf,
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// No result
			await receiveStop(readPin);
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

		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'remove_service',
					port,
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// No result
			await receiveStop(readPin);
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
		let publicKey: Uint8Array;
		try {
			publicKey = mnemonicToEntropy(pubkey_phrase, wordlist);
		}
		catch (error) {
			console.log('Not a valid BIP39 phrase:');
			console.log(pubkey_phrase);
			console.log(error instanceof Error ? error.message : error);
			return;
		}

		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'add_remote',
					publicKey,
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// No result
			await receiveStop(readPin);
		});
	}

	if (argv[2] === 'deny') {
		const pubkey_phrase = argv.slice(3).join(' ').trim();
		if (pubkey_phrase === '') {
			console.log('Please specify a public key as a BIP39 mnemonic phrase');
			console.log();
			console.log('Usage:');
			console.log('  deny <public key>');
			return;
		}
		let publicKey: Uint8Array;
		try {
			publicKey = mnemonicToEntropy(pubkey_phrase, wordlist);
		}
		catch (error) {
			console.log('Not a valid BIP39 phrase:');
			console.log(pubkey_phrase);
			console.log(error instanceof Error ? error.message : error);
			return;
		}

		return await runProcesses(async (processes, optionsProc) => {
			const { readPin, writePin: commandWritePin } = createPipe<Uint8Array>();
			const { readPin: commandReadPin, writePin } = createPipe<Uint8Array>();

			await sendValue(processes, async (options) => {
				await runCommand(commandReadPin, commandWritePin, options);
			}, optionsProc);

			// Send command
			await sendValue(processes, async (options) => {
				await sendCmdRequest(writePin, {
					type: 'remove_remote',
					publicKey,
				}, options);
				await sendFinish(writePin, optionsProc);
			}, optionsProc);

			// No result
			await receiveStop(readPin);
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
