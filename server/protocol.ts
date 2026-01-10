/**
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

import type {
	Abort,
} from './pin-stream/abort.ts';
import {
	sendValue,
} from './pin-stream/pin-stream.ts';
import type {
	PipeReadPin,
	PipeWritePin,
} from './pin-stream/pin-stream.ts';
import {
	decodePacketStream,
	readPacket,
	transformPacketStream,
} from './pin-stream/transform.ts';
import type {
	PacketProcessResult,
} from './pin-stream/transform.ts';
import type {
	ActiveProxy,
} from './proxy-manager.ts';

export type ServerRequestPacket = {
	type: 'list',
} | {
	type: 'proxy',
	serviceName: Uint8Array,
};

export type CmdRequestPacket = {
	type: 'show_key',
} | {
	type: 'show_services',
} | {
	type: 'show_remotes',
} | {
	type: 'show_servers',
} | {
	type: 'show_proxies',
} | {
	type: 'add_service',
	name: Uint8Array,
	host?: string,
	port: number,
} | {
	type: 'remove_service',
	name: Uint8Array,
} | {
	type: 'add_remote',
	publicKey: Uint8Array,
} | {
	type: 'remove_remote',
	publicKey: Uint8Array,
} | {
	type: 'add_server',
	name: string,
	publicKey: Uint8Array,
} | {
	type: 'remove_server',
	name: string,
} | {
	type: 'list',
	serverName: string,
} | {
	type: 'proxy',
	serverName: string,
	serviceName: Uint8Array,
	port: number,
} | {
	type: 'unproxy',
	port: number,
};

export type CmdResponsePacket = {
	type: 'show_key',
	publicKey: Uint8Array,
} | {
	type: 'proxy',
	port: number,
};

export async function receiveServerRequest(input: PipeReadPin<Uint8Array>, options: {
	abort?: Abort,
	serviceLengthChecker?: (length: number) => Promise<void>,
	servicePrefixChecker?: (length: number, prefix: Uint8Array) => Promise<void>,
} = {}): Promise<ServerRequestPacket> {
	const {
		serviceLengthChecker,
		servicePrefixChecker,
	} = options;

	return await readPacket(input, async (buffer): Promise<PacketProcessResult<ServerRequestPacket>> => {
		if (buffer.length < 1) {
			// Need more data
			return {
				complete: false,
			};
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const messageType = buffer[0];

		switch (messageType) {
			case 0: {	// list
				return {
					complete: true,
					packet: {
						type: 'list',
					},
					bytesUsed: 1,
				};
			}

			case 1: {	// proxy
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const serviceNameLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + serviceNameLength) {
					if (buffer.length > 5) {
						if (servicePrefixChecker) {
							await servicePrefixChecker(serviceNameLength, buffer.subarray(5));
						}
						else if (serviceLengthChecker) {
							await serviceLengthChecker(serviceNameLength);
						}
					}
					else if (serviceLengthChecker) {
						await serviceLengthChecker(serviceNameLength);
					}
					// Need more data
					return {
						complete: false,
					};
				}

				const serviceName = buffer.subarray(5, 5 + serviceNameLength);

				return {
					complete: true,
					packet: {
						type: 'proxy',
						serviceName,
					},
					bytesUsed: 5 + serviceNameLength,
				};
			}

			default:
				throw new Error(`Unknown request type ${ messageType }`);
		}
	}, options);
}

export async function sendServerRequest(output: PipeWritePin<Uint8Array>, request: ServerRequestPacket, options?: {
	abort?: Abort,
	throwOnNoMore?: boolean,
}): Promise<boolean> {
	// Encode the message
	let encoded: Uint8Array;

	switch (request.type) {
		case 'list': {
			encoded = new Uint8Array(1);
			encoded[0] = 0;
			break;
		}

		case 'proxy': {
			encoded = new Uint8Array(5 + request.serviceName.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 1;
			dataview.setUint32(1, request.serviceName.length, true);
			encoded.set(request.serviceName, 5);
			break;
		}
	}

	return await sendValue(output, encoded, options);
}

export async function receiveServerServiceList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<Uint8Array>> => {
		if (buffer.length < 4) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const length = dataview.getUint32(0, true);

		if (buffer.length < 4 + length) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: buffer.subarray(4, 4 + length),
			bytesUsed: 4 + length,
		});
	}, options);
}

export async function sendServerServiceList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, (serviceName: Uint8Array): Promise<Uint8Array> => {
		const encoded: Uint8Array = new Uint8Array(4 + serviceName.length);
		const dataview = new DataView(encoded.buffer);
		dataview.setUint32(0, serviceName.length, true);
		encoded.set(serviceName, 4);
		return Promise.resolve(encoded);
	}, options);
}

export async function receiveCmdRequest(input: PipeReadPin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<CmdRequestPacket> {
	return await readPacket(input, async (buffer): Promise<PacketProcessResult<CmdRequestPacket>> => {
		if (buffer.length < 1) {
			// Need more data
			return {
				complete: false,
			};
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const messageType = buffer[0];

		switch (messageType) {
			case 0: {	// show_key
				return {
					complete: true,
					packet: {
						type: 'show_key',
					},
					bytesUsed: 1,
				};
			}

			case 1: {	// show_services
				return {
					complete: true,
					packet: {
						type: 'show_services',
					},
					bytesUsed: 1,
				};
			}

			case 2: {	// show_remotes
				return {
					complete: true,
					packet: {
						type: 'show_remotes',
					},
					bytesUsed: 1,
				};
			}

			case 3: {	// show_servers
				return {
					complete: true,
					packet: {
						type: 'show_servers',
					},
					bytesUsed: 1,
				};
			}

			case 4: {	// show_proxies
				return {
					complete: true,
					packet: {
						type: 'show_proxies',
					},
					bytesUsed: 1,
				};
			}

			case 5: {	// add_service - no host
				if (buffer.length < 7) {
					// Need more data
					return {
						complete: false,
					};
				}

				const port = dataview.getUint16(1, true);
				const nameLength = dataview.getUint32(3, true);

				if (buffer.length < 7 + nameLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'add_service',
						name: buffer.subarray(7, 7 + nameLength),
						port,
					},
					bytesUsed: 7 + nameLength,
				};
			}

			case 6: {	// add_service - with host
				if (buffer.length < 11) {
					// Need more data
					return {
						complete: false,
					};
				}

				const port = dataview.getUint16(1, true);
				const nameLength = dataview.getUint32(3, true);
				const hostLength = dataview.getUint32(7, true);

				if (buffer.length < 11 + nameLength + hostLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'add_service',
						name: buffer.subarray(11, 11 + nameLength),
						host: Buffer.from(buffer.subarray(11 + nameLength, 11 + nameLength + hostLength)).toString(),
						port,
					},
					bytesUsed: 11 + nameLength + hostLength,
				};
			}

			case 7: {	// remove_service
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const nameLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + nameLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'remove_service',
						name: buffer.subarray(5, 5 + nameLength),
					},
					bytesUsed: 5 + nameLength,
				};
			}

			case 8: {	// add_remote
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const publicKeyLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + publicKeyLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'add_remote',
						publicKey: buffer.subarray(5, 5 + publicKeyLength),
					},
					bytesUsed: 5 + publicKeyLength,
				};
			}

			case 9: {	// remove_remote
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const publicKeyLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + publicKeyLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'remove_remote',
						publicKey: buffer.subarray(5, 5 + publicKeyLength),
					},
					bytesUsed: 5 + publicKeyLength,
				};
			}

			case 10: {	// add_server
				if (buffer.length < 9) {
					// Need more data
					return {
						complete: false,
					};
				}

				const nameLength = dataview.getUint32(1, true);
				const publicKeyLength = dataview.getUint32(5, true);

				if (buffer.length < 9 + nameLength + publicKeyLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'add_server',
						name: Buffer.from(buffer.subarray(9, 9 + nameLength)).toString(),
						publicKey: buffer.subarray(9 + nameLength, 9 + nameLength + publicKeyLength),
					},
					bytesUsed: 9 + nameLength + publicKeyLength,
				};
			}

			case 11: {	// remove_server
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const nameLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + nameLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'remove_server',
						name: Buffer.from(buffer.subarray(5, 5 + nameLength)).toString(),
					},
					bytesUsed: 5 + nameLength,
				};
			}

			case 12: {	// list
				if (buffer.length < 5) {
					// Need more data
					return {
						complete: false,
					};
				}

				const serverNameLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + serverNameLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'list',
						serverName: Buffer.from(buffer.subarray(5, 5 + serverNameLength)).toString(),
					},
					bytesUsed: 5 + serverNameLength,
				};
			}

			case 13: {	// proxy
				if (buffer.length < 11) {
					// Need more data
					return {
						complete: false,
					};
				}

				const port = dataview.getUint16(1, true);
				const serverNameLength = dataview.getUint32(3, true);
				const serviceNameLength = dataview.getUint32(7, true);

				if (buffer.length < 11 + serverNameLength + serviceNameLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'proxy',
						serverName: Buffer.from(buffer.subarray(11, 11 + serverNameLength)).toString(),
						serviceName: buffer.subarray(11 + serverNameLength, 11 + serverNameLength + serviceNameLength),
						port,
					},
					bytesUsed: 11 + serverNameLength + serviceNameLength,
				};
			}

			case 14: {	// unproxy
				if (buffer.length < 3) {
					// Need more data
					return {
						complete: false,
					};
				}

				const port = dataview.getUint16(1, true);

				return {
					complete: true,
					packet: {
						type: 'unproxy',
						port,
					},
					bytesUsed: 3,
				};
			}

			default:
				throw new Error(`Unknown request type ${ messageType }`);
		}
	}, options);
}

export async function sendCmdRequest(output: PipeWritePin<Uint8Array>, request: CmdRequestPacket, options?: {
	abort?: Abort,
	throwOnNoMore?: boolean,
}): Promise<boolean> {
	// Encode the message
	let encoded: Uint8Array;

	switch (request.type) {
		case 'show_key': {
			encoded = new Uint8Array(1);
			encoded[0] = 0;
			break;
		}

		case 'show_services': {
			encoded = new Uint8Array(1);
			encoded[0] = 1;
			break;
		}

		case 'show_remotes': {
			encoded = new Uint8Array(1);
			encoded[0] = 2;
			break;
		}

		case 'show_servers': {
			encoded = new Uint8Array(1);
			encoded[0] = 3;
			break;
		}

		case 'show_proxies': {
			encoded = new Uint8Array(1);
			encoded[0] = 4;
			break;
		}

		case 'add_service': {
			const hostBuffer = request.host !== undefined ? Buffer.from(request.host) : null;
			encoded = new Uint8Array(7 + request.name.length + (hostBuffer ? 4 + hostBuffer.length : 0));
			const dataview = new DataView(encoded.buffer);
			encoded[0] = hostBuffer ? 6 : 5;
			dataview.setUint16(1, request.port, true);
			dataview.setUint32(3, request.name.length, true);
			if (hostBuffer) {
				dataview.setUint32(7, hostBuffer.length, true);
			}
			encoded.set(request.name, hostBuffer ? 11 : 7);
			if (hostBuffer) {
				encoded.set(hostBuffer, 11 + request.name.length);
			}
			break;
		}

		case 'remove_service': {
			encoded = new Uint8Array(5 + request.name.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 7;
			dataview.setUint32(1, request.name.length, true);
			encoded.set(request.name, 5);
			break;
		}

		case 'add_remote': {
			encoded = new Uint8Array(5 + request.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 8;
			dataview.setUint32(1, request.publicKey.length, true);
			encoded.set(request.publicKey, 5);
			break;
		}

		case 'remove_remote': {
			encoded = new Uint8Array(5 + request.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 9;
			dataview.setUint32(1, request.publicKey.length, true);
			encoded.set(request.publicKey, 5);
			break;
		}

		case 'add_server': {
			const nameBuffer = Buffer.from(request.name);
			encoded = new Uint8Array(9 + nameBuffer.length + request.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 10;
			dataview.setUint32(1, nameBuffer.length, true);
			dataview.setUint32(5, request.publicKey.length, true);
			encoded.set(nameBuffer, 9);
			encoded.set(request.publicKey, 9 + nameBuffer.length);
			break;
		}

		case 'remove_server': {
			const nameBuffer = Buffer.from(request.name);
			encoded = new Uint8Array(5 + nameBuffer.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 11;
			dataview.setUint32(1, nameBuffer.length, true);
			encoded.set(nameBuffer, 5);
			break;
		}

		case 'list': {
			const serverNameBuffer = Buffer.from(request.serverName);
			encoded = new Uint8Array(5 + serverNameBuffer.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 12;
			dataview.setUint32(1, serverNameBuffer.length, true);
			encoded.set(serverNameBuffer, 5);
			break;
		}

		case 'proxy': {
			const serverNameBuffer = Buffer.from(request.serverName);
			encoded = new Uint8Array(11 + serverNameBuffer.length + request.serviceName.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 13;
			dataview.setUint16(1, request.port, true);
			dataview.setUint32(3, serverNameBuffer.length, true);
			dataview.setUint32(7, request.serviceName.length, true);
			encoded.set(serverNameBuffer, 11);
			encoded.set(request.serviceName, 11 + serverNameBuffer.length);
			break;
		}

		case 'unproxy': {
			encoded = new Uint8Array(3);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 14;
			dataview.setUint16(1, request.port, true);
			break;
		}
	}

	return await sendValue(output, encoded, options);
}

export async function receiveCmdResponse<const T extends CmdResponsePacket['type']>(input: PipeReadPin<Uint8Array>, requestType: T, options?: {
	abort?: Abort,
}): Promise<CmdResponsePacket & { type: T }> {
	return await readPacket(input, async (buffer): Promise<PacketProcessResult<CmdResponsePacket & { type: T }>> => {
		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

		switch (requestType) {
			case 'show_key': {
				if (buffer.length < 4) {
					// Need more data
					return {
						complete: false,
					};
				}

				const publicKeyLength = dataview.getUint32(0, true);

				if (buffer.length < 4 + publicKeyLength) {
					// Need more data
					return {
						complete: false,
					};
				}

				return {
					complete: true,
					packet: {
						type: 'show_key',
						publicKey: buffer.subarray(4, 4 + publicKeyLength),
					} satisfies CmdResponsePacket as (CmdResponsePacket & { type: T }),
					bytesUsed: 4 + publicKeyLength,
				};
			}

			case 'proxy': {
				if (buffer.length < 2) {
					// Need more data
					return {
						complete: false,
					};
				}

				const port = dataview.getUint16(0, true);

				return {
					complete: true,
					packet: {
						type: 'proxy',
						port,
					} satisfies CmdResponsePacket as (CmdResponsePacket & { type: T }),
					bytesUsed: 2,
				};
			}
		}
	}, options);
}

export async function sendCmdResponse(output: PipeWritePin<Uint8Array>, response: CmdResponsePacket, options?: {
	abort?: Abort,
	throwOnNoMore?: boolean,
}): Promise<boolean> {
	// Encode the message
	let encoded: Uint8Array;

	switch (response.type) {
		case 'show_key': {
			encoded = new Uint8Array(4 + response.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			dataview.setUint32(0, response.publicKey.length, true);
			encoded.set(response.publicKey, 4);
			break;
		}

		case 'proxy': {
			encoded = new Uint8Array(2);
			const dataview = new DataView(encoded.buffer);
			dataview.setUint16(0, response.port, true);
			break;
		}
	}

	return await sendValue(output, encoded, options);
}

export async function receiveCmdServiceList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<{
	name: Uint8Array,
	host?: string,
	port: number,
}>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<{
		name: Uint8Array,
		host?: string,
		port: number,
	}>> => {
		if (buffer.length < 7) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const port = dataview.getUint16(0, true);
		const nameLength = dataview.getUint32(2, true);
		const hasHost = buffer[6] !== 0;

		if (hasHost) {
			if (buffer.length < 11) {
				// Need more data
				return Promise.resolve({
					complete: false,
				});
			}

			const hostLength = dataview.getUint32(7, true);

			if (buffer.length < 11 + nameLength + hostLength) {
				// Need more data
				return Promise.resolve({
					complete: false,
				});
			}

			return Promise.resolve({
				complete: true,
				packet: {
					name: buffer.subarray(11, 11 + nameLength),
					host: Buffer.from(buffer.subarray(11 + nameLength, 11 + nameLength + hostLength)).toString(),
					port,
				},
				bytesUsed: 11 + nameLength + hostLength,
			});
		}

		if (buffer.length < 7 + nameLength) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: {
				name: buffer.subarray(7, 7 + nameLength),
				port,
			},
			bytesUsed: 7 + nameLength,
		});
	}, options);
}

export async function sendCmdServiceList(input: PipeReadPin<{
	name: Uint8Array,
	host?: string,
	port: number,
}>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, ({
		name,
		host,
		port,
	}): Promise<Uint8Array> => {
		const hostBuffer = host ? Buffer.from(host) : null;
		const encoded: Uint8Array = new Uint8Array(7 + name.length + (hostBuffer ? hostBuffer.length + 4 : 0));
		const dataview = new DataView(encoded.buffer);
		dataview.setUint16(0, port, true);
		dataview.setUint32(2, name.length, true);
		if (hostBuffer) {
			encoded[6] = 1;
			dataview.setUint32(7, hostBuffer.length, true);
			encoded.set(name, 11);
			encoded.set(hostBuffer, 11 + name.length);
		}
		else {
			encoded[6] = 0;
			encoded.set(name, 7);
		}
		return Promise.resolve(encoded);
	}, options);
}

export async function receiveCmdRemoteList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<Uint8Array>> => {
		if (buffer.length < 4) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const length = dataview.getUint32(0, true);

		if (buffer.length < 4 + length) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: buffer.subarray(4, 4 + length),
			bytesUsed: 4 + length,
		});
	}, options);
}

export async function sendCmdRemoteList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, (publicKey: Uint8Array): Promise<Uint8Array> => {
		const encoded: Uint8Array = new Uint8Array(4 + publicKey.length);
		const dataview = new DataView(encoded.buffer);
		dataview.setUint32(0, publicKey.length, true);
		encoded.set(publicKey, 4);
		return Promise.resolve(encoded);
	}, options);
}

export async function receiveCmdServerList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<{
	name: string,
	publicKey: Uint8Array,
}>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<{
		name: string,
		publicKey: Uint8Array,
	}>> => {
		if (buffer.length < 8) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const nameLength = dataview.getUint32(0, true);
		const publicKeyLength = dataview.getUint32(4, true);

		if (buffer.length < 8 + nameLength + publicKeyLength) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: {
				name: Buffer.from(buffer.subarray(8, 8 + nameLength)).toString(),
				publicKey: buffer.subarray(8 + nameLength, 8 + nameLength + publicKeyLength),
			},
			bytesUsed: 8 + nameLength + publicKeyLength,
		});
	}, options);
}

export async function sendCmdServerList(input: PipeReadPin<{
	name: string,
	publicKey: Uint8Array,
}>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, ({
		name,
		publicKey,
	}): Promise<Uint8Array> => {
		const nameBuffer = Buffer.from(name);
		const encoded: Uint8Array = new Uint8Array(8 + nameBuffer.length + publicKey.length);
		const dataview = new DataView(encoded.buffer);
		dataview.setUint32(0, nameBuffer.length, true);
		dataview.setUint32(4, publicKey.length, true);
		encoded.set(nameBuffer, 8);
		encoded.set(publicKey, 8 + nameBuffer.length);
		return Promise.resolve(encoded);
	}, options);
}

export async function receiveCmdProxyList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<ActiveProxy>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<ActiveProxy>> => {
		if (buffer.length < 10) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const port = dataview.getUint16(0, true);
		const serverNameLength = dataview.getUint32(2, true);
		const serviceNameLength = dataview.getUint32(6, true);

		if (buffer.length < 10 + serverNameLength + serviceNameLength) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: {
				port,
				serverName: Buffer.from(buffer.subarray(10, 10 + serverNameLength)).toString(),
				serviceName: buffer.subarray(10 + serverNameLength, 10 + serverNameLength + serviceNameLength),
			},
			bytesUsed: 10 + serverNameLength + serviceNameLength,
		});
	}, options);
}

export async function sendCmdProxyList(input: PipeReadPin<ActiveProxy>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, ({
		port,
		serverName,
		serviceName,
	}): Promise<Uint8Array> => {
		const serverNameBuffer = Buffer.from(serverName);
		const encoded: Uint8Array = new Uint8Array(10 + serverNameBuffer.length + serviceName.length);
		const dataview = new DataView(encoded.buffer);
		dataview.setUint16(0, port, true);
		dataview.setUint32(2, serverNameBuffer.length, true);
		dataview.setUint32(6, serviceName.length, true);
		encoded.set(serverNameBuffer, 10);
		encoded.set(serviceName, 10 + serverNameBuffer.length);
		return Promise.resolve(encoded);
	}, options);
}
