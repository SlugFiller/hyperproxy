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

import DHT from 'hyperdht';
import {
	type Types,
} from '@callstack/licenses';
import {
	scanDependencies,
} from '@callstack/licenses/node';
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from '@scure/bip39';
import {
	wordlist,
} from '@scure/bip39/wordlists/english.js';
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
	drainReadable,
} from './drain-readable.ts';
import {
	Abort,
	anyAbort,
} from './pin-stream/abort.ts';
import {
	toAsyncIterable,
} from './pin-stream/iterable.ts';
import {
	type PipeReadPin,
	type PipeWritePin,
	createClosedReadPin,
	createPipe,
	receiveStop,
	sendFinish,
	sendValue,
} from './pin-stream/pin-stream.ts';
import {
	processReadable,
	processWritable,
} from './pin-stream/pin-stream-compat.ts';
import {
	Processes,
} from './pin-stream/processes.ts';
import {
	streamSplitter,
	type StreamSplitterStream,
} from './pin-stream/stream-splitter.ts';
import {
	waitTimeout,
} from './pin-stream/timeout.ts';
import {
	receiveCmdProxyList,
	receiveCmdRemoteList,
	receiveCmdRequest,
	receiveCmdResponse,
	receiveCmdServerList,
	receiveCmdServiceList,
	receiveServerRequest,
	receiveServerServiceList,
	sendCmdProxyList,
	sendCmdRemoteList,
	sendCmdRequest,
	sendCmdResponse,
	sendCmdServerList,
	sendCmdServiceList,
	sendServerRequest,
	sendServerServiceList,
} from './protocol.ts';
import {
	type ActiveProxy,
	ProxyManager,
} from './proxy-manager.ts';

const CMD_PATH = platform === 'win32' ? '\\\\.\\pipe\\hyperproxy-daemon' : '/tmp/hyperproxy-daemon.sock';

async function daemon(noServe: boolean, abort: Abort): Promise<void> {
	using db = new DatabaseSync(fileURLToPath(new URL('config.sqlite', import.meta.url)));

	db.exec(`CREATE TABLE IF NOT EXISTS keypair(
		publicKey BLOB NOT NULL,
		secretKey BLOB NOT NULL
	)`);
	db.exec(`CREATE TABLE IF NOT EXISTS remotes(
		publicKey BLOB NOT NULL
	)`);
	db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS remotes__pubkey ON remotes (publicKey)`);
	db.exec(`CREATE TABLE IF NOT EXISTS services(
		name BLOB NOT NULL,
		host TEXT,
		port INTEGER NOT NULL
	)`);
	db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS services__name_unique ON services (name)`);
	db.exec(`CREATE INDEX IF NOT EXISTS services__name ON services (LENGTH(name), name)`);
	db.exec(`CREATE TABLE IF NOT EXISTS servers(
		name TEXT,
		publicKey BLOB
	)`);
	db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS servers__name ON servers (name)`);

	const queryKeyPair = db.prepare(`SELECT publicKey, secretKey FROM keypair LIMIT 1`);
	const queryKeyPairStore = db.prepare(`INSERT INTO keypair(publicKey, secretKey) SELECT $publicKey, $secretKey
		FROM (SELECT 1 t) t LEFT JOIN keypair e ON (1=1) WHERE e.ROWID IS NULL`);
	const queryCheckRemote = db.prepare(`SELECT 1 FROM remotes WHERE publicKey = $publicKey`);
	const queryListServices = db.prepare(`SELECT name FROM services ORDER BY name ASC`);
	const queryCheckServiceLength = db.prepare(`SELECT 1 FROM services WHERE LENGTH(name) = $length LIMIT 1`);
	const queryCheckServicePrefix = db.prepare(`SELECT 1 FROM services WHERE LENGTH(name) = $length AND name >= $prefix AND name < $prefixPlus LIMIT 1`);
	const queryGetServiceByName = db.prepare(`SELECT host, port FROM services WHERE LENGTH(name) = $length AND name = $match LIMIT 1`);
	const queryGetServices = db.prepare(`SELECT name, host, port FROM services ORDER BY name ASC`);
	const queryGetRemotes = db.prepare(`SELECT publicKey FROM remotes ORDER BY publicKey ASC`);
	const queryGetServerByName = db.prepare(`SELECT publicKey FROM servers WHERE name = $name LIMIT 1`);
	const queryGetServers = db.prepare(`SELECT name, publicKey FROM servers ORDER BY name ASC`);
	const queryAddService = db.prepare(`REPLACE INTO services(host, port, name) VALUES ($host, $port, $name)`);
	const queryRemoveService = db.prepare(`DELETE FROM services WHERE name = $name`);
	const queryAddRemote = db.prepare(`INSERT OR IGNORE INTO remotes(publicKey) VALUES ($publicKey)`);
	const queryRemoveRemote = db.prepare(`DELETE FROM remotes WHERE publicKey = $publicKey`);
	const queryAddServer = db.prepare(`REPLACE INTO servers(name, publicKey) VALUES ($name, $publicKey)`);
	const queryRemoveServer = db.prepare(`DELETE FROM servers WHERE name = $name`);

	await using nodeStack = new AsyncDisposableStack();
	const node = nodeStack.adopt(new DHT(), async (r) => {
		await r.destroy();
	});

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
			publicKey: new Uint8Array(dbKey.publicKey),
			secretKey: new Uint8Array(dbKey.secretKey),
		};
	}

	await using processesConnections = new Processes();
	{
		await using proxyManager = new ProxyManager({ abort: processesConnections.abort });

		await using serverStack = new AsyncDisposableStack();
		const server = serverStack.adopt(node.createServer({
			firewall: (remotePublicKey: Uint8Array): boolean => {
				const row = queryCheckRemote.get({
					$publicKey: remotePublicKey,
				});
				return !row;
			}
		}), async (r) => {
			await r.close();
		});

		server.on('error', (error: unknown) => {
			// The default behavior for `EventEmitter` is to crash the VM if an error
			// is emitted without a handler. This handler pre-emptively prevents this
			console.log('DHT server error', error);
		});

		server.on('connection', (socket) => {
			socket.on('error', (error: unknown) => {
				// The default behavior for `EventEmitter` is to crash the VM if an error
				// is emitted without a handler. This handler pre-emptively prevents this
				console.log('DHT socket error', error);
			});
			processesConnections.run(async () => {
				try {
					using socketStack = new DisposableStack();
					socketStack.adopt(socket, (r) => {
						r.destroy();
					});
					await using processes = new Processes({ abort: processesConnections.abort });
					const {
						abort,
					} = processes;

					const { writePin: socketOutput, readPin } = createPipe<Uint8Array>();
					const { writePin, readPin: socketInput } = createPipe<Uint8Array>();

					processes.run(async () => {
						await processReadable(socket, socketOutput, { abort });
					});

					processes.run(async () => {
						await processWritable(socketInput, socket, { abort });
					});

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
							const prefixPlus = new Uint8Array(prefix);
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
							await receiveStop(readPin, { abort });

							const { writePin: listOutput, readPin: listEncodeInput } = createPipe<Uint8Array>();
							const listEncodeOutput = writePin;

							processes.run(async () => {
								await sendServerServiceList(listEncodeInput, listEncodeOutput, { abort });
							});

							for (const row of queryListServices.iterate()) {
								await sendValue(listOutput, (row as {
									name: Uint8Array,
								}).name, { abort });
							}

							await sendFinish(listOutput, { abort });

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
								host: Uint8Array | null,
								port: number,
							});
							if (!row) {
								throw new Error('Service does not exist');
							}
							const port = row.port;
							const host = row.host ? Buffer.from(row.host).toString() : 'localhost';

							const remoteStreams = createPipe<StreamSplitterStream>();

							processes.run(async () => {
								await streamSplitter(readPin, writePin, createClosedReadPin<StreamSplitterStream>(), remoteStreams.writePin, { abort });
							});

							for await (const {
								readPin: connectionReadPin,
								writePin: connectionWritePin,
								localAbort,
								remoteAbort,
							} of toAsyncIterable(remoteStreams.readPin, { abort })) {
								processes.run(async () => {
									try {
										await using proxied = createConnection(port, host);
										proxied.on('error', (error: unknown) => {
											// The default behavior for `EventEmitter` is to crash the VM if an error
											// is emitted without a handler. This handler pre-emptively prevents this
											console.log('Proxied socket error', error);
										});

										await using processesConn = new Processes({
											abort,
											aborts: [
												// Also abort on remote abort
												remoteAbort,
											],
										});

										// Bridge connection
										processesConn.run(async () => {
											await processReadable(proxied, connectionWritePin, { abort: processesConn.abort });
										});

										await processWritable(connectionReadPin, proxied, { abort: processesConn.abort });

										await processesConn.finish();
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

							break;
						}
					}


					await processes.finish();
				}
				catch (error) {
					console.log(error);
				}
			});
		});

		if (!noServe) {
			await server.listen(keyPair);
		}

		await using cmd = createServer();
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
			processesConnections.run(async () => {
				try {
					await using _ = socket;
					await using processes = new Processes({ abort: processesConnections.abort });

					const { writePin: socketOutput, readPin } = createPipe<Uint8Array>();
					const { writePin, readPin: socketInput } = createPipe<Uint8Array>();

					processes.run(async () => {
						await processReadable(socket, socketOutput, { abort: processes.abort });
					});

					processes.run(async () => {
						await processWritable(socketInput, socket, { abort: processes.abort });
					});

					const request = await receiveCmdRequest(readPin, { abort: processes.abort });

					await receiveStop(readPin, { abort: processes.abort });

					switch (request.type) {
						case 'show_key': {
							await sendCmdResponse(writePin, {
								type: 'show_key',
								publicKey: keyPair.publicKey,
							}, { abort: processes.abort });
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'show_services': {
							const { writePin: listOutput, readPin: listEncodeInput } = createPipe<{
								host?: string,
								port: number,
								name: Uint8Array,
							}>();
							const listEncodeOutput = writePin;

							processes.run(async () => {
								await sendCmdServiceList(listEncodeInput, listEncodeOutput, { abort: processes.abort });
							});

							for (const row of queryGetServices.iterate()) {
								const service = row as {
									name: Uint8Array,
									host: string | null,
									port: number,
								};
								await sendValue(listOutput, service.host === null ? {
									name: service.name,
									port: service.port,
								} : {
									name: service.name,
									host: service.host,
									port: service.port,
								}, { abort: processes.abort });
							}

							await sendFinish(listOutput, { abort: processes.abort });
							break;
						}

						case 'show_remotes': {
							const { writePin: listOutput, readPin: listEncodeInput } = createPipe<Uint8Array>();
							const listEncodeOutput = writePin;

							processes.run(async () => {
								await sendCmdRemoteList(listEncodeInput, listEncodeOutput, { abort: processes.abort });
							});

							for (const row of queryGetRemotes.iterate()) {
								await sendValue(listOutput, (row as {
									publicKey: Uint8Array,
								}).publicKey, { abort: processes.abort });
							}

							await sendFinish(listOutput, { abort: processes.abort });
							break;
						}

						case 'show_servers': {
							const { writePin: listOutput, readPin: listEncodeInput } = createPipe<{
								name: string,
								publicKey: Uint8Array,
							}>();
							const listEncodeOutput = writePin;

							processes.run(async () => {
								await sendCmdServerList(listEncodeInput, listEncodeOutput, { abort: processes.abort });
							});

							for (const row of queryGetServers.iterate()) {
								await sendValue(listOutput, row as {
									name: string,
									publicKey: Uint8Array,
								}, { abort: processes.abort });
							}

							await sendFinish(listOutput, { abort: processes.abort });
							break;
						}

						case 'show_proxies': {
							const { writePin: listOutput, readPin: listEncodeInput } = createPipe<ActiveProxy>();
							const listEncodeOutput = writePin;

							processes.run(async () => {
								await sendCmdProxyList(listEncodeInput, listEncodeOutput, { abort: processes.abort });
							});

							for (const proxy of proxyManager.getActiveProxies()) {
								await sendValue(listOutput, proxy, { abort: processes.abort });
							}

							await sendFinish(listOutput, { abort: processes.abort });
							break;
						}

						case 'add_service': {
							queryAddService.run({
								$host: request.host ?? null,
								$port: request.port,
								$name: request.name,
							});
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'remove_service': {
							queryRemoveService.run({
								$name: request.name,
							});
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'add_remote': {
							queryAddRemote.run({
								$publicKey: request.publicKey,
							});
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'remove_remote': {
							queryRemoveRemote.run({
								$publicKey: request.publicKey,
							});
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'add_server': {
							queryAddServer.run({
								$name: request.name,
								$publicKey: request.publicKey,
							});
							// In case a previous server with a different name was overwritten
							proxyManager.removeProxiesByServer(request.name);
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'remove_server': {
							queryRemoveServer.run({
								$name: request.name,
							});
							proxyManager.removeProxiesByServer(request.name);
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'list': {
							const row = queryGetServerByName.get({
								$name: request.serverName,
							}) as (undefined | {
								publicKey: Uint8Array,
							});
							if (!row) {
								throw new Error('Server does not exist');
							}

							const { writePin: requestOutput, readPin: commandInput } = createPipe<Uint8Array>();
							// Stream the result directly to the write pin
							const commandOutput = writePin;

							// Connect to server
							using socketStack = new DisposableStack();
							const socketConn = socketStack.adopt(node.connect(row.publicKey, {
								keyPair,
							}), (r) => {
								r.destroy();
							});
							socketConn.on('error', (error: unknown) => {
								// The default behavior for `EventEmitter` is to crash the VM if an error
								// is emitted without a handler. This handler pre-emptively prevents this
								console.log('Request socket error', error);
							});

							await using processesConn = new Processes({ abort: processes.abort });

							processesConn.run(async () => {
								await processReadable(socketConn, commandOutput, { abort: processesConn.abort });
							});

							processesConn.run(async () => {
								await processWritable(commandInput, socketConn, { abort: processesConn.abort });
							});

							// Send the request packet
							await sendServerRequest(requestOutput, {
								type: 'list',
							}, { abort: processesConn.abort });

							await sendFinish(requestOutput, { abort: processesConn.abort });

							await processesConn.finish();

							break;
						}

						case 'proxy': {
							const row = queryGetServerByName.get({
								$name: request.serverName,
							}) as (undefined | {
								publicKey: Uint8Array,
							});
							if (!row) {
								throw new Error('Server does not exist');
							}

							const port = await proxyManager.addProxy(node, keyPair, request.serverName, row.publicKey, request.serviceName, request.port);
							await sendCmdResponse(writePin, {
								type: 'proxy',
								port,
							}, { abort: processes.abort });
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}

						case 'unproxy': {
							proxyManager.removeProxyByPort(request.port);
							await sendFinish(writePin, { abort: processes.abort });
							break;
						}
					}

					await processes.finish();
				}
				catch (error) {
					console.log(error);
				}
			});
		});

		cmd.listen({
			path: CMD_PATH,
			readableAll: true,
			writableAll: true,
		});

		await anyAbort(abort);
	}

	console.log('Politely asking daemon to stop...');
	try {
		await using processesTimeout = new Processes({ abort: processesConnections.abort });
		processesTimeout.run(async () => {
			await waitTimeout(10000, { abort: processesTimeout.abort });
			throw new Error('Timed out being polite');
		});
		await processesConnections.finish({ abort: processesTimeout.abort });
	}
	catch {
	}
	if (!processesConnections.finished) {
		console.log('Being less polite...');
	}
}

async function runCommand(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	await using socket = createConnection(CMD_PATH);
	await using processes = new Processes({ abort: options.abort })

	// Bridge connection
	processes.run(async () => {
		await processReadable(socket, output, { abort: processes.abort });
	});

	await processWritable(input, socket, { abort: processes.abort });

	await processes.finish();
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

async function app(abort: Abort): Promise<void> {
	if (argv[2] === 'daemon') {
		return await daemon(argv[3] === 'noserve', abort);
	}

	if (argv[2] === 'show' && argv[3] === 'key') {
		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'show_key',
		}, { abort: processes.abort });

		// Print result
		const result = await receiveCmdResponse(readPin, 'show_key', { abort: processes.abort });
		printKey(result.publicKey);

		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'show' && argv[3] === 'services') {
		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin: listDecodeInput } = createPipe<Uint8Array>();
		const { writePin: listDecodeOutput, readPin: listInput } = createPipe<{
			name: Uint8Array,
			host?: string,
			port: number,
		}>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'show_services',
		}, { abort: processes.abort });

		// Receive list
		processes.run(async () => {
			await receiveCmdServiceList(listDecodeInput, listDecodeOutput, { abort: processes.abort });
		});

		// Print result
		for await (const {
			name,
			host,
			port,
		} of toAsyncIterable(listInput, { abort: processes.abort })) {
			if (host) {
				console.log(`${ host } ${ port.toString().padStart(8) } ${ Buffer.from(name).toString() }`);
			}
			else {
				console.log(`${ port.toString().padStart(8) } ${ Buffer.from(name).toString() }`);
			}
		}

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'show' && argv[3] === 'allowed') {
		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin: listDecodeInput } = createPipe<Uint8Array>();
		const { writePin: listDecodeOutput, readPin: listInput } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'show_remotes',
		}, { abort: processes.abort });

		// Receive list
		processes.run(async () => {
			await receiveCmdRemoteList(listDecodeInput, listDecodeOutput, { abort: processes.abort });
		});

		// Print result
		for await (const publicKey of toAsyncIterable(listInput, { abort: processes.abort, sendStop: 'never' })) {
			printKey(publicKey);
			break;
		}
		for await (const publicKey of toAsyncIterable(listInput, { abort: processes.abort })) {
			console.log();
			printKey(publicKey);
		}

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'show' && argv[3] === 'servers') {
		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin: listDecodeInput } = createPipe<Uint8Array>();
		const { writePin: listDecodeOutput, readPin: listInput } = createPipe<{
			name: string,
			publicKey: Uint8Array,
		}>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'show_servers',
		}, { abort: processes.abort });

		// Receive list
		processes.run(async () => {
			await receiveCmdServerList(listDecodeInput, listDecodeOutput, { abort: processes.abort });
		});

		// Print result
		for await (const { name, publicKey } of toAsyncIterable(listInput, { abort: processes.abort, sendStop: 'never' })) {
			console.log(`Name: ${ name }`);
			printKey(publicKey);
			break;
		}
		for await (const { name, publicKey } of toAsyncIterable(listInput, { abort: processes.abort })) {
			console.log();
			console.log(`Name: ${ name }`);
			printKey(publicKey);
		}

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'show' && argv[3] === 'proxies') {
		const decoder = new TextDecoder('utf-8');
		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin: listDecodeInput } = createPipe<Uint8Array>();
		const { writePin: listDecodeOutput, readPin: listInput } = createPipe<ActiveProxy>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'show_proxies',
		}, { abort: processes.abort });

		// Receive list
		processes.run(async () => {
			await receiveCmdProxyList(listDecodeInput, listDecodeOutput, { abort: processes.abort });
		});

		// Print result
		for await (const { port, serverName, serviceName } of toAsyncIterable(listInput, { abort: processes.abort, sendStop: 'never' })) {
			console.log(`Port: ${ port }`);
			console.log(`Server: ${ serverName }`);
			console.log(`Service: ${ decoder.decode(serviceName) }`);
			break;
		}
		for await (const { port, serverName, serviceName } of toAsyncIterable(listInput, { abort: processes.abort })) {
			console.log();
			console.log(`Port: ${ port }`);
			console.log(`Server: ${ serverName }`);
			console.log(`Service: ${ decoder.decode(serviceName) }`);
		}

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'add') {
		let port_arg = 3;
		if (!/^[0-9]+$/.test(argv[3])) {
			if (!/^[0-9]+$/.test(argv[4])) {
				console.log('Please specify a port');
				console.log();
				console.log('Usage:');
				console.log('  add [<host>] <port> <name>');
				return;
			}
			port_arg = 4;
		}
		const name = argv.slice(port_arg + 1).join(' ').trim();
		if (name === '') {
			console.log('Please specify a name');
			console.log();
			console.log('Usage:');
			console.log('  add <port> <name>');
			return;
		}
		const host = port_arg === 4 ? argv[3] : null;
		const port = parseInt(argv[port_arg]);
		const nameBuf = Buffer.from(name);
		if (port < 1 || port > 65535) {
			console.log('Port must be between 1 and 65535');
			return;
		}

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, host === null ? {
			type: 'add_service',
			port,
			name: nameBuf,
		} : {
			type: 'add_service',
			host,
			port,
			name: nameBuf,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'remove') {
		const name = argv.slice(3).join(' ').trim();
		if (name === '') {
			console.log('Please specify the name of the service');
			console.log();
			console.log('Usage:');
			console.log('  remove <name>');
			return;
		}
		const nameBuf = Buffer.from(name);

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'remove_service',
			name: nameBuf,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
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

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'add_remote',
			publicKey,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
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

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'remove_remote',
			publicKey,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'register') {
		const name = (argv[3] ?? '').trim();
		if (name === '') {
			console.log('Please specify a name for the server');
			console.log();
			console.log('Usage:');
			console.log('  register <name> <public key>');
			return;
		}
		const pubkey_phrase = argv.slice(4).join(' ').trim();
		if (pubkey_phrase === '') {
			console.log('Please specify a public key as a BIP39 mnemonic phrase');
			console.log();
			console.log('Usage:');
			console.log('  register <name> <public key>');
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

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'add_server',
			name,
			publicKey,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'unregister') {
		const name = argv.slice(3).join(' ').trim();
		if (name === '') {
			console.log('Please specify the name of the server');
			console.log();
			console.log('Usage:');
			console.log('  unregister <name>');
			return;
		}

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'remove_server',
			name,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'list') {
		const decoder = new TextDecoder('utf-8');
		const serverName = argv.slice(3).join(' ').trim();
		if (serverName === '') {
			console.log('Please specify the name of the server');
			console.log();
			console.log('Usage:');
			console.log('  list <server name>');
			return;
		}

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin: listDecodeInput } = createPipe<Uint8Array>();
		const { writePin: listDecodeOutput, readPin: listInput } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'list',
			serverName,
		}, { abort: processes.abort });

		// Receive list
		processes.run(async () => {
			await receiveServerServiceList(listDecodeInput, listDecodeOutput, { abort: processes.abort });
		});

		// Print result
		for await (const serviceName of toAsyncIterable(listInput, { abort: processes.abort })) {
			console.log(`${ decoder.decode(serviceName) }`);
		}

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'connect') {
		const serverName = (argv[3] ?? '').trim();
		if (serverName === '') {
			console.log('Please specify the name of the server');
			console.log();
			console.log('Usage:');
			console.log('  connect <server name> <service name>');
			return;
		}

		const serviceName = argv.slice(4).join(' ').trim();
		if (serviceName === '') {
			console.log('Please specify the name of the service');
			console.log();
			console.log('Usage:');
			console.log('  connect <server name> <service name>');
			return;
		}
		const encoder = new TextEncoder();
		const serviceNameBuf = encoder.encode(serviceName);

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'proxy',
			serverName,
			serviceName: serviceNameBuf,
			port: 0,
		}, { abort: processes.abort });

		// Print result
		const result = await receiveCmdResponse(readPin, 'proxy', { abort: processes.abort });
		console.log(`Proxy listening on port ${ result.port }`);

		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'port' && argv[4] === 'connect') {
		if (!/^[0-9]+$/.test(argv[3])) {
			console.log('Please specify a port');
			console.log();
			console.log('Usage:');
			console.log('  port <port> connect <server name> <service name>');
			return;
		}

		const serverName = (argv[5] ?? '').trim();
		if (serverName === '') {
			console.log('Please specify the name of the server');
			console.log();
			console.log('Usage:');
			console.log('  port <port> connect <server name> <service name>');
			return;
		}

		const serviceName = argv.slice(6).join(' ').trim();
		if (serviceName === '') {
			console.log('Please specify the name of the service');
			console.log();
			console.log('Usage:');
			console.log('  port <port> connect <server name> <service name>');
			return;
		}
		const encoder = new TextEncoder();
		const serviceNameBuf = encoder.encode(serviceName);

		const port = parseInt(argv[3]);
		if (port < 1 || port > 65535) {
			console.log('Port must be between 1 and 65535');
			return;
		}

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'proxy',
			serverName,
			serviceName: serviceNameBuf,
			port,
		}, { abort: processes.abort });

		// Print result
		const result = await receiveCmdResponse(readPin, 'proxy', { abort: processes.abort });
		console.log(`Proxy listening on port ${ result.port }`);

		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'disconnect') {
		if (!/^[0-9]+$/.test(argv[3])) {
			console.log('Please specify a port');
			console.log();
			console.log('Usage:');
			console.log('  port <port> connect <server name> <service name>');
			return;
		}

		const port = parseInt(argv[3]);
		if (port < 1 || port > 65535) {
			console.log('Port must be between 1 and 65535');
			return;
		}

		await using processes = new Processes({ abort });

		const { writePin, readPin: commandInput } = createPipe<Uint8Array>();
		const { writePin: commandOutput, readPin } = createPipe<Uint8Array>();

		processes.run(async () => {
			await runCommand(commandInput, commandOutput, { abort: processes.abort });
		});

		// Send command
		await sendCmdRequest(writePin, {
			type: 'unproxy',
			port,
		}, { abort: processes.abort });

		// No result
		await receiveStop(readPin, { abort: processes.abort });

		// Finish must be delayed until after read, because pipes can't be half-closed
		await sendFinish(writePin, { abort: processes.abort });

		await processes.finish();
		return;
	}

	if (argv[2] === 'licenses') {
		const optionsFactory: Types.ScanPackageOptionsFactory = ({ isRoot }) => ({
			includeDevDependencies: isRoot,
			includeTransitiveDependencies: true,
			includeOptionalDependencies: true,
		});

		const licenses = scanDependencies(fileURLToPath(new URL('package.json', import.meta.url)), optionsFactory);

		for (const [, {
			name,
			version,
			author,
			type,
			content,
		}] of Object.entries(licenses)) {
			console.log();
			console.log('Package:', name);
			console.log('Version:', version);
			if (author) {
				console.log('Author:', author);
			}
			console.log('License:', type);
			if (content) {
				console.log('License text:');
				console.log(content);
			}
		}

		return;
	}

	console.log('Commands:');
	console.log('  daemon [noserve]');
	console.log('    Start the daemon and listen for connections and commands');
	console.log('    If "noserve" is specified, the daemon does not listen to client');
	console.log('    connections, and may be used purely as a client');
	console.log('    Note: None of the other commands will work of a daemon has not been started');
	console.log('  show key');
	console.log('    Show the public key');
	console.log('  show services');
	console.log('    Show the list of available services');
	console.log('  show allowed');
	console.log('    Show the list of allowed remote public keys');
	console.log('  show servers');
	console.log('    Show the list of registered servers');
	console.log('  show proxies');
	console.log('    Show the list of active proxies');
	console.log('  add [<host>] <port> <name>');
	console.log('    Add the specified service to the list of services offered by this server');
	console.log('    If host is not specified, localhost is used');
	console.log('  remove <name>');
	console.log('    Remove the specified service from the service list');
	console.log('  allow <public key>');
	console.log('    Add the specified public key to the list of clients allowed to');
	console.log('    connect to this server');
	console.log('  deny <public key>');
	console.log('    Remove the specified public key from the list of clients allowed to');
	console.log('    connect to this server');
	console.log('  register <name> <public key>');
	console.log('    Register the specified public key as a new server');
	console.log('  unregister <name>');
	console.log('    Remove the specified server from the registered servers list');
	console.log('  list <server name>');
	console.log('    List service names of services offered by the specified server');
	console.log('  [port <port>] connect <server name> <service name>');
	console.log('    Connect to the specified service on the specified server and proxy');
	console.log('    any connections to a local port to that service');
	console.log('    If a port is not specified, a random local port is used');
	console.log('  disconnect <port>');
	console.log('    Disconnect a previously established connection to a server');
	console.log('  licenses');
	console.log('    Display licenses of libraries used in this software');
}

const userAbort = new Abort();
const userAbortError = new Error('User interrupt received');
process.once('SIGINT', () => userAbort.abort(userAbortError));
process.once('SIGTERM', () => userAbort.abort(userAbortError));

await app(userAbort);
