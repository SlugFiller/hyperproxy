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

import {
	type Abort,
} from '../src/pin-stream/abort.ts';
import {
	type PipeReadPin,
	type PipeWritePin,
	sendValue,
} from '../src/pin-stream/pin-stream.ts';
import {
	type PacketProcessResult,
	decodePacketStream,
	readPacket,
} from '../src/pin-stream/transform.ts';

export type RPCRequestPacket = {
	type: 'keygen',
} | {
	type: 'list',
	publicKey: Uint8Array,
	secretKey: Uint8Array,
	serverKey: Uint8Array,
} | {
	type: 'proxy',
	port: number,
	publicKey: Uint8Array,
	secretKey: Uint8Array,
	serverKey: Uint8Array,
	serviceName: Uint8Array,
} | {
	type: 'unproxy',
	serverKey: Uint8Array,
	serviceName: Uint8Array,
} | {
	type: 'is_proxy',
	serverKey: Uint8Array,
	serviceName: Uint8Array,
} | {
	type: 'unproxy_all',
	serverKey: Uint8Array,
};

export type RPCResponsePacket = {
	type: 'keygen',
	publicKey: Uint8Array,
	secretKey: Uint8Array,
} | {
	type: 'proxy',
	port: number,
} | {
	type: 'is_proxy',
	port: number,
};

export type ServerRequestPacket = {
	type: 'list',
} | {
	type: 'proxy',
	serviceName: Uint8Array,
};

export async function receiveRPCRequest(input: PipeReadPin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<RPCRequestPacket> {
	return await readPacket(input, (buffer): Promise<PacketProcessResult<RPCRequestPacket>> => {
		if (buffer.length < 1) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const messageType = buffer[0];

		switch (messageType) {
			case 0: {	// keygen
				return Promise.resolve({
					complete: true,
					packet: {
						type: 'keygen',
					},
					bytesUsed: 1,
				});
			}

			case 1: {	// list
				if (buffer.length < 13) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const publicKeyLength = dataview.getUint32(1, true);
				const secretKeyLength = dataview.getUint32(5, true);
				const serverKeyLength = dataview.getUint32(9, true);

				if (buffer.length < 13 + publicKeyLength + secretKeyLength + serverKeyLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'list',
						publicKey: buffer.subarray(13, 13 + publicKeyLength),
						secretKey: buffer.subarray(13 + publicKeyLength, 13 + publicKeyLength + secretKeyLength),
						serverKey: buffer.subarray(13 + publicKeyLength + secretKeyLength, 13 + publicKeyLength + secretKeyLength + serverKeyLength),
					},
					bytesUsed: 13 + publicKeyLength + secretKeyLength + serverKeyLength,
				});
			}

			case 2: {	// proxy
				if (buffer.length < 19) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const port = dataview.getUint16(1, true);
				const publicKeyLength = dataview.getUint32(3, true);
				const secretKeyLength = dataview.getUint32(7, true);
				const serverKeyLength = dataview.getUint32(11, true);
				const serviceNameLength = dataview.getUint32(15, true);

				if (buffer.length < 19 + publicKeyLength + secretKeyLength + serverKeyLength + serviceNameLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'proxy',
						port,
						publicKey: buffer.subarray(19, 19 + publicKeyLength),
						secretKey: buffer.subarray(19 + publicKeyLength, 19 + publicKeyLength + secretKeyLength),
						serverKey: buffer.subarray(19 + publicKeyLength + secretKeyLength, 19 + publicKeyLength + secretKeyLength + serverKeyLength),
						serviceName: buffer.subarray(19 + publicKeyLength + secretKeyLength + serverKeyLength, 19 + publicKeyLength + secretKeyLength + serverKeyLength + serviceNameLength),
					},
					bytesUsed: 19 + publicKeyLength + secretKeyLength + serverKeyLength + serviceNameLength,
				});
			}

			case 3: {	// unproxy
				if (buffer.length < 9) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const serverKeyLength = dataview.getUint32(1, true);
				const serviceNameLength = dataview.getUint32(5, true);

				if (buffer.length < 9 + serverKeyLength + serviceNameLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'unproxy',
						serverKey: buffer.subarray(9, 9 + serverKeyLength),
						serviceName: buffer.subarray(9 + serverKeyLength, 9 + serverKeyLength + serviceNameLength),
					},
					bytesUsed: 9 + serverKeyLength + serviceNameLength,
				});
			}

			case 4: {	// is_proxy
				if (buffer.length < 9) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const serverKeyLength = dataview.getUint32(1, true);
				const serviceNameLength = dataview.getUint32(5, true);

				if (buffer.length < 9 + serverKeyLength + serviceNameLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'is_proxy',
						serverKey: buffer.subarray(9, 9 + serverKeyLength),
						serviceName: buffer.subarray(9 + serverKeyLength, 9 + serverKeyLength + serviceNameLength),
					},
					bytesUsed: 9 + serverKeyLength + serviceNameLength,
				});
			}

			case 5: {	// unproxy_all
				if (buffer.length < 5) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const serverKeyLength = dataview.getUint32(1, true);

				if (buffer.length < 5 + serverKeyLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'unproxy_all',
						serverKey: buffer.subarray(5, 5 + serverKeyLength),
					},
					bytesUsed: 5 + serverKeyLength,
				});
			}

			default:
				throw new Error(`Unknown request type ${ messageType }`);
		}
	}, options);
}

export async function sendRPCRequest(output: PipeWritePin<Uint8Array>, request: RPCRequestPacket, options?: {
	abort?: Abort,
	throwOnNoMore?: boolean,
}): Promise<boolean> {
	// Encode the message
	let encoded: Uint8Array;

	switch (request.type) {
		case 'keygen': {
			encoded = new Uint8Array(1);
			encoded[0] = 0;
			break;
		}

		case 'list': {
			encoded = new Uint8Array(13 + request.publicKey.length + request.secretKey.length + request.serverKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 1;
			dataview.setUint32(1, request.publicKey.length, true);
			dataview.setUint32(5, request.secretKey.length, true);
			dataview.setUint32(9, request.serverKey.length, true);
			encoded.set(request.publicKey, 13);
			encoded.set(request.secretKey, 13 + request.publicKey.length);
			encoded.set(request.serverKey, 13 + request.publicKey.length + request.secretKey.length);
			break;
		}

		case 'proxy': {
			encoded = new Uint8Array(19 + request.publicKey.length + request.secretKey.length + request.serverKey.length + request.serviceName.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 2;
			dataview.setUint16(1, request.port, true);
			dataview.setUint32(3, request.publicKey.length, true);
			dataview.setUint32(7, request.secretKey.length, true);
			dataview.setUint32(11, request.serverKey.length, true);
			dataview.setUint32(15, request.serviceName.length, true);
			encoded.set(request.publicKey, 19);
			encoded.set(request.secretKey, 19 + request.publicKey.length);
			encoded.set(request.serverKey, 19 + request.publicKey.length + request.secretKey.length);
			encoded.set(request.serviceName, 19 + request.publicKey.length + request.secretKey.length + request.serverKey.length);
			break;
		}

		case 'unproxy': {
			encoded = new Uint8Array(9 + request.serverKey.length + request.serviceName.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 3;
			dataview.setUint32(1, request.serverKey.length, true);
			dataview.setUint32(5, request.serviceName.length, true);
			encoded.set(request.serverKey, 9);
			encoded.set(request.serviceName, 9 + request.serverKey.length);
			break;
		}

		case 'is_proxy': {
			encoded = new Uint8Array(9 + request.serverKey.length + request.serviceName.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 4;
			dataview.setUint32(1, request.serverKey.length, true);
			dataview.setUint32(5, request.serviceName.length, true);
			encoded.set(request.serverKey, 9);
			encoded.set(request.serviceName, 9 + request.serverKey.length);
			break;
		}

		case 'unproxy_all': {
			encoded = new Uint8Array(5 + request.serverKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 5;
			dataview.setUint32(1, request.serverKey.length, true);
			encoded.set(request.serverKey, 5);
			break;
		}
	}

	return await sendValue(output, encoded, options);
}

export async function receiveRPCResponse<const T extends RPCResponsePacket['type']>(input: PipeReadPin<Uint8Array>, requestType: T, options?: {
	abort?: Abort,
}): Promise<RPCResponsePacket & { type: T }> {
	return await readPacket(input, (buffer): Promise<PacketProcessResult<RPCResponsePacket & { type: T }>> => {
		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

		switch (requestType) {
			case 'keygen': {
				if (buffer.length < 8) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const publicKeyLength = dataview.getUint32(0, true);
				const secretKeyLength = dataview.getUint32(4, true);

				if (buffer.length < 8 + publicKeyLength + secretKeyLength) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'keygen',
						publicKey: buffer.subarray(8, 8 + publicKeyLength),
						secretKey: buffer.subarray(8 + publicKeyLength, 9 + publicKeyLength + secretKeyLength),
					} satisfies RPCResponsePacket as (RPCResponsePacket & { type: T }),
					bytesUsed: 8 + publicKeyLength + secretKeyLength,
				});
			}

			case 'proxy': {
				if (buffer.length < 4) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const port = dataview.getUint32(0, true);

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'proxy',
						port,
					} satisfies RPCResponsePacket as (RPCResponsePacket & { type: T }),
					bytesUsed: 4,
				});
			}

			case 'is_proxy': {
				if (buffer.length < 4) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}

				const port = dataview.getUint32(0, true);

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'is_proxy',
						port,
					} satisfies RPCResponsePacket as (RPCResponsePacket & { type: T }),
					bytesUsed: 4,
				});
			}
		}
	}, options);
}

export async function sendRPCResponse(output: PipeWritePin<Uint8Array>, response: RPCResponsePacket, options?: {
	abort?: Abort,
	throwOnNoMore?: boolean,
}): Promise<boolean> {
	// Encode the message
	let encoded: Uint8Array;

	switch (response.type) {
		case 'keygen': {
			encoded = new Uint8Array(8 + response.publicKey.length + response.secretKey.length);
			const dataview = new DataView(encoded.buffer);
			dataview.setUint32(0, response.publicKey.length, true);
			dataview.setUint32(4, response.secretKey.length, true);
			encoded.set(response.publicKey, 8);
			encoded.set(response.secretKey, 8 + response.publicKey.length);
			break;
		}

		case 'proxy': {
			encoded = new Uint8Array(4);
			const dataview = new DataView(encoded.buffer);
			dataview.setUint32(0, response.port, true);
			break;
		}

		case 'is_proxy': {
			encoded = new Uint8Array(4);
			const dataview = new DataView(encoded.buffer);
			dataview.setUint32(0, response.port, true);
			break;
		}
	}

	return await sendValue(output, encoded, options);
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
