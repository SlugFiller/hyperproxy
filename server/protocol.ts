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
	type: 'add_service',
	port: number,
	name: Uint8Array,
} | {
	type: 'remove_service',
	port: number,
} | {
	type: 'add_remote',
	publicKey: Uint8Array,
} | {
	type: 'remove_remote',
	publicKey: Uint8Array,
};

export type CmdResponsePacket = {
	type: 'show_key',
	publicKey: Uint8Array,
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

			case 3: {	// add_service
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
						port,
						name: buffer.subarray(7, 7 + nameLength),
					},
					bytesUsed: 7 + nameLength,
				};
			}

			case 4: {	// remove_service
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
						type: 'remove_service',
						port,
					},
					bytesUsed: 3,
				};
			}

			case 5: {	// add_remote
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
						type: 'add_remote',
						publicKey: buffer.subarray(5, 5 + nameLength),
					},
					bytesUsed: 5 + nameLength,
				};
			}

			case 6: {	// remove_remote
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
						type: 'remove_remote',
						publicKey: buffer.subarray(5, 5 + nameLength),
					},
					bytesUsed: 5 + nameLength,
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

		case 'add_service': {
			encoded = new Uint8Array(7 + request.name.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 3;
			dataview.setUint16(1, request.port, true);
			dataview.setUint32(3, request.name.length, true);
			encoded.set(request.name, 7);
			break;
		}

		case 'remove_service': {
			encoded = new Uint8Array(3);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 4;
			dataview.setUint16(1, request.port, true);
			break;
		}

		case 'add_remote': {
			encoded = new Uint8Array(5 + request.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 5;
			dataview.setUint32(1, request.publicKey.length, true);
			encoded.set(request.publicKey, 5);
			break;
		}

		case 'remove_remote': {
			encoded = new Uint8Array(5 + request.publicKey.length);
			const dataview = new DataView(encoded.buffer);
			encoded[0] = 6;
			dataview.setUint32(1, request.publicKey.length, true);
			encoded.set(request.publicKey, 5);
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
	}

	return await sendValue(output, encoded, options);
}

export async function receiveCmdServiceList(input: PipeReadPin<Uint8Array>, output: PipeWritePin<{
	port: number,
	name: Uint8Array,
}>, options?: {
	abort?: Abort,
}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<{
		port: number,
		name: Uint8Array,
	}>> => {
		if (buffer.length < 6) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const port = dataview.getUint16(0, true);
		const length = dataview.getUint32(2, true);

		if (buffer.length < 6 + length) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		return Promise.resolve({
			complete: true,
			packet: {
				port,
				name: buffer.subarray(6, 6 + length),
			},
			bytesUsed: 6 + length,
		});
	}, options);
}

export async function sendCmdServiceList(input: PipeReadPin<{
	port: number,
	name: Uint8Array,
}>, output: PipeWritePin<Uint8Array>, options?: {
	abort?: Abort,
}): Promise<void> {
	await transformPacketStream(input, output, ({
		port,
		name,
	}): Promise<Uint8Array> => {
		const encoded: Uint8Array = new Uint8Array(6 + name.length);
		const dataview = new DataView(encoded.buffer);
		dataview.setUint16(0, port, true);
		dataview.setUint32(2, name.length, true);
		encoded.set(name, 6);
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
