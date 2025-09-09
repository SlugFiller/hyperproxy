import b4a from 'b4a';
import {
	addWeakListener,
} from './weak-event.mjs'

const kSignalListener = Symbol('kSignalListener')
const kRefHolder = Symbol('kRefHolder')

export function combineStreams(...streams) {
	return async function* (source, { signal } = {}) {
		for (const stream of streams) {
			const prev = source;
			source = async function* ({ signal } = {}) {
				return yield* stream(prev, { signal });
			};
		}
		return yield* source({ signal });
	}
}

export async function runStream(stream, { signal } = {}) {
	async function* source() {
	}

	const iter = stream(source, { signal });
	try {
		while (true) {
			const { done, value } = await iter.next();
			if (done) {
				// Return the return value from the stream if it ended
				return value;
			}
			// Otherwise, discard the value
		}
	}
	finally {
		await iter.return();
	}
}

class EventSignal {
	#controller;
	#listener;

	constructor(object, event) {
		this.#controller = new AbortController();
		this.#listener = (err) => {
			this.#controller.abort(err);
		};
		addWeakListener(object, event, this.#listener);
	}

	reset() {
		if (this.#controller.signal.aborted) {
			this.#controller = new AbortController();
		}
	}

	get signal() {
		return this.#controller.signal;
	}

	static wait(signal) {
		if (signal.aborted) {
			return Promise.resolve();
		}
		return new Promise((resolve) => {
			if (signal.aborted) {
				return resolve();
			}
			signal.addEventListener('abort', resolve);
		});
	}
}

export function readerFromNodeStream(stream) {
	return (source, { signal } = {}) => {
		const controller = new AbortController();
		if (signal) {
			if (signal.aborted) {
				stream.destroy(signal.reason);
				return (async function* () {
					throw signal.reason;
				})();
			}
			controller[kSignalListener] = () => {
				controller.abort(signal.reason);
			};
			addWeakListener(signal, 'abort', controller[kSignalListener], { once: true });
		}

		controller.signal.addEventListener('abort', () => stream.destroy(controller.signal.reason));

		const sigReadable = new EventSignal(stream, 'readable');
		const sigError = new EventSignal(stream, 'error');
		const sigClose = new EventSignal(stream, 'close');

		sigError.signal.addEventListener('abort', () => controller.abort(sigError.signal.reason));

		const ret = (async function* () {
			try {
				// The input doesn't matter. Discard it
				source && await source({ signal: AbortSignal.abort() }).return();

				while (true) {
					controller.signal.throwIfAborted();
					sigReadable.reset();
					const data = stream.read();
					if (data !== null) {
						yield data;
						continue;
					}
					await EventSignal.wait(AbortSignal.any([sigReadable.signal, sigClose.signal, controller.signal]));
					controller.signal.throwIfAborted();
					if (!sigReadable.signal.aborted) {
						break;
					}
				}
			}
			catch (e) {
				controller.abort(e);
				throw e;
			}
		})();

		ret[kRefHolder] = [sigReadable, sigError, sigClose, stream, controller];

		return ret;
	};
}

export function writerFromNodeStream(stream) {
	return (source, { signal } = {}) => {
		const controllerFinal = new AbortController();
		const controller = new AbortController();
		if (signal) {
			if (signal.aborted) {
				stream.destroy(signal.reason);
				return (async function* () {
					throw signal.reason;
				})();
			}
			controller[kSignalListener] = () => {
				controller.abort(signal.reason);
			};
			addWeakListener(signal, 'abort', controller[kSignalListener], { once: true });
		}

		controller.signal.addEventListener('abort', () => stream.destroy(controller.signal.reason));

		const sigDrain = new EventSignal(stream, 'drain');
		const sigError = new EventSignal(stream, 'error');
		const sigClose = new EventSignal(stream, 'close');

		sigError.signal.addEventListener('abort', () => controller.abort(sigError.signal.reason));

		// Start writer
		// Don't wait for any read request
		// The read side is only used for extracting error or close signal
		(async () => {
			for await (const packet of source({ signal: controller.signal })) {
				sigDrain.reset();
				if (!stream.write(packet)) {
					await EventSignal.wait(AbortSignal.any([sigDrain.signal, sigClose.signal, controller.signal]));
					controller.signal.throwIfAborted();
					if (!sigDrain.signal.aborted) {
						throw new Error('Destination unexpectedly closed');
					}
				}
			}
			stream.end();
			await EventSignal.wait(AbortSignal.any([sigClose.signal, controller.signal]));
			// No need to throw on controller abort here. It would just translate back to the controller
		})().catch((error) => {
			controller.abort(error);
		}).then(() => {
			controllerFinal.abort();
		});

		const ret = (async function* () {
			await EventSignal.wait(controllerFinal.signal);
			controller.signal.throwIfAborted();
		})();

		ret[kRefHolder] = [sigDrain, sigError, sigClose, stream, controller];

		return ret;
	};
}

export async function* streamPacketer(stream) {
	const iter = stream[Symbol.asyncIterator]();
	try {
		let buffer = b4a.alloc(0);
		let returnSame = false;
		yield {
			consume(amount) {
				buffer = buffer.subarray(amount);
				if (buffer.byteLength > 0) {
					returnSame = true;
				}
			},
			async next() {
				if (returnSame) {
					returnSame = false;
					return { done: false, value: buffer };
				}
				const { done, value } = await iter.next();
				if (done) {
					return { done: true, value: undefined };
				}
				buffer = b4a.concat([buffer, value]);
				return { done: false, value: buffer };
			},
			throw(e) {
				return Promise.reject(e);
			},
			return() {
				if (buffer.byteLength > 0) {
					returnSame = true;
				}
				return Promise.resolve({ done: true, value: undefined });
			},
			[Symbol.asyncIterator]() {
				return this;
			},
		};
	}
	finally {
		await iter.return();
	}
}

export async function anyPacket(packeter) {
	for await (const packet of packeter) {
		return true;
	}
	return false;
}

export async function consumeUInt32LE(packeter) {
	for await (const packet of packeter) {
		if (packet.length >= 4) {
			packeter.consume(4);
			return b4a.readUInt32LE(packet);
		}
	}
	throw new Error('Unexpected end of stream');
}

export async function consumeBuffer(packeter) {
	const len = await consumeUInt32LE(packeter);
	for await (const packet of packeter) {
		if (packet.length >= len) {
			packeter.consume(len);
			return packet.subarray(0, len);
		}
	}
	throw new Error('Unexpected end of stream');
}

export function packetUInt32LE(value) {
	const packet = b4a.alloc(4);
	b4a.writeUInt32LE(packet, value);
	return packet;
}

export class StreamSplitter {
	#onStream;
	#pendingStreams;
	#pendingStreamsHandler;

	constructor(onStream) {
		this.#onStream = onStream;
		this.#pendingStreams = [];
		this.#pendingStreamsHandler = null;
	}

	createStream(stream) {
		if (this.#pendingStreamsHandler) {
			this.#pendingStreamsHandler(stream);
		}
		else {
			this.#pendingStreams.push(stream);
		}
	}

	get split() {
		const substreamsRemote = new Map();
		const substreamsLocal = [];
		const substreamsLocalFreeList = [];
		const controller = new AbortController();
		const sender = new PacketSender({ signal: controller.signal });

		const MSG_REMOTE_NEW = 0;
		const MSG_REMOTE_NEXT = 1;
		const MSG_REMOTE_RETURN = 2;
		const MSG_REMOTE_PACKET = 3;
		const MSG_REMOTE_THROW = 4;
		const MSG_REMOTE_DONE = 5;
		const MSG_REMOTE_FINAL = 6;
		const MSG_LOCAL_NEXT = 7;
		const MSG_LOCAL_RETURN = 8;
		const MSG_LOCAL_PACKET = 9;
		const MSG_LOCAL_THROW = 10;
		const MSG_LOCAL_DONE = 11;
		const MSG_LOCAL_FINAL = 12;
		const MSG_LOCAL_CLOSED = 13;

		function runStream(stream_id, substream, streamFunc, msg_next, msg_return, msg_packet, msg_throw, msg_done, msg_final, send_closed) {
			let returnSent = new AbortController();
			let returnReceived = false;
			let finalReceived = false;
			let isDone = false;
			function sendReturn() {
				if (returnSent.signal.aborted) {
					return;
				}
				returnSent.abort();
				sender.push({ done: false, value: packetUInt32LE(msg_return) });
				sender.push({ done: false, value: packetUInt32LE(stream_id) });
			}
			(async () => {
				async function* source({ streamSignal } = {}) {
					if (streamSignal) {
						if (streamSignal.aborted) {
							sendReturn();
						}
						else {
							substream.signal_listener = () => {
								sendReturn();
							};
							addWeakListener(streamSignal, 'abort', substream.signal_listener, { once: true });
						}
					}
					try {
						while (!returnSent.signal.aborted) {
							sender.push({ done: false, value: packetUInt32LE(msg_next) });
							sender.push({ done: false, value: packetUInt32LE(stream_id) });
							const recv = await substream.sender.shift();
							if (recv.throw) {
								throw packet.error;
							}
							if (recv.done) {
								if (recv.final) {
									finalReceived = true;
								}
								break;
							}
							yield recv.value;
						}
					}
					finally {
						sendReturn();
					}
				}
				const iter = streamFunc(source, { signal: AbortSignal.any([controller.signal, substream.controller.signal]) });
				try {
					// Run the iterator right away, to give it a chance to request a packet
					for await (const packet of iter) {
						// Wait for the other side to request a packet before sending it
						if (!await substream.receiver.shift()) {
							returnReceived = true;
							break;
						}
						sender.push({ done: false, value: packetUInt32LE(msg_packet) });
						sender.push({ done: false, value: packetUInt32LE(stream_id) });
						sender.push({ done: false, value: packetUInt32LE(packet.byteLength) });
						sender.push({ done: false, value: packet });
					}
					sender.push({ done: false, value: packetUInt32LE(msg_done) });
					sender.push({ done: false, value: packetUInt32LE(stream_id) });
				}
				catch (e) {
					controller.signal.throwIfAborted();
					if (!await substream.receiver.shift()) {
						returnReceived = true;
					}
					const msgBuf = b4a.from(e.message);
					sender.push({ done: false, value: packetUInt32LE(msg_throw) });
					sender.push({ done: false, value: packetUInt32LE(stream_id) });
					sender.push({ done: false, value: packetUInt32LE(msgBuf.byteLength) });
					sender.push({ done: false, value: msgBuf });
				}
				finally {
					// Send 'return' if it hasn't been sent already
					sendReturn();
					// Wait for 'return' to be received
					while (!returnReceived) {
						if (!await substream.receiver.shift()) {
							returnReceived = true;
						}
					}
					// Send 'final'. No more messages can be sent after this
					sender.push({ done: false, value: packetUInt32LE(msg_final) });
					sender.push({ done: false, value: packetUInt32LE(stream_id) });
					// Wait for 'final' to be received
					while (!finalReceived) {
						const recv = await substream.sender.shift();
						if (recv.final) {
							finalReceived = true;
						}
					}
					if (send_closed) {
						substreamsRemote.delete(stream_id);
						// Tell the remote it is now allowed to reuse the stream id
						sender.push({ done: false, value: packetUInt32LE(MSG_LOCAL_CLOSED) });
						sender.push({ done: false, value: packetUInt32LE(stream_id) });
					}
				}
			})().catch((error) => {
				sender.push({ throw: true, error });
			});
		}

		this.#pendingStreamsHandler = (streamFunc) => {
			const substream = {
				sender: new PacketSender({ signal: controller.signal }),
				receiver: new PacketSender({ signal: controller.signal }),
				controller: new AbortController(),
			};
			let stream_id;
			if (substreamsLocalFreeList.length) {
				stream_id = substreamsLocalFreeList.pop();
				substreamsLocal[stream_id] = substream;
			}
			else {
				stream_id = substreamsLocal.length;
				substreamsLocal.push(substream);
			}
			// Inform remote we have a new stream
			sender.push({ done: false, value: packetUInt32LE(MSG_REMOTE_NEW) });
			sender.push({ done: false, value: packetUInt32LE(stream_id) });
			runStream(stream_id, substream, streamFunc, MSG_REMOTE_NEXT, MSG_REMOTE_RETURN, MSG_REMOTE_PACKET, MSG_REMOTE_THROW, MSG_REMOTE_DONE, MSG_REMOTE_FINAL, false);
		};
		const pendingStreams = this.#pendingStreams;
		this.#pendingStreams = [];
		for (const pendingStream of pendingStreams) {
			this.#pendingStreamsHandler(pendingStream);
		}

		const onStream = this.#onStream || async function* () {
		};

		return async function* (stream, { signal } = {}) {
			if (signal) {
				if (signal.aborted) {
					throw signal.reason;
				}
				controller[kSignalListener] = () => {
					controller.abort(signal.reason);
				};
				addWeakListener(signal, 'abort', controller[kSignalListener], { once: true });
			}

			(async () => {
				for await (const packeter of streamPacketer(stream({ signal: controller.signal }))) {
					while (await anyPacket(packeter)) {
						const type = await consumeUInt32LE(packeter);
						switch (type) {
							case MSG_REMOTE_NEW: {
								const stream_id = await consumeUInt32LE(packeter);
								if (substreamsRemote.has(stream_id)) {
									throw new Error(`Received remote stream with already used id ${ stream_id }`);
								}
								const substream = {
									sender: new PacketSender({ signal: controller.signal }),
									receiver: new PacketSender({ signal: controller.signal }),
									controller: new AbortController(),
								};
								substreamsRemote.set(stream_id, substream);
								runStream(stream_id, substream, onStream, MSG_LOCAL_NEXT, MSG_LOCAL_RETURN, MSG_LOCAL_PACKET, MSG_LOCAL_THROW, MSG_LOCAL_DONE, MSG_LOCAL_FINAL, true);
								break;
							}
							case MSG_REMOTE_NEXT: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								substream.receiver.push(true);
								break;
							}
							case MSG_REMOTE_RETURN: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								substream.receiver.push(false);
								substream.controller.abort();
								break;
							}
							case MSG_REMOTE_PACKET: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								const value = await consumeBuffer(packeter);
								substream.sender.push({ done: false, value });
								break;
							}
							case MSG_REMOTE_THROW: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								const message = b4a.toString(await consumeBuffer(packeter));
								substream.sender.push({ throw: true, error: new Error(message) });
								break;
							}
							case MSG_REMOTE_DONE: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								substream.sender.push({ done: true });
								break;
							}
							case MSG_REMOTE_FINAL: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsRemote.get(stream_id);
								if (!substream) {
									throw new Error(`No remote substream for stream id ${ stream_id }`);
								}
								substream.sender.push({ done: true, final: true });
								break;
							}
							case MSG_LOCAL_NEXT: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								substream.receiver.push(true);
								break;
							}
							case MSG_LOCAL_RETURN: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								substream.receiver.push(false);
								substream.controller.abort();
								break;
							}
							case MSG_LOCAL_PACKET: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								const value = await consumeBuffer(packeter);
								substream.sender.push({ done: false, value });
								break;
							}
							case MSG_LOCAL_THROW: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								const message = b4a.toString(await consumeBuffer(packeter));
								substream.sender.push({ throw: true, error: new Error(message) });
								break;
							}
							case MSG_LOCAL_DONE: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								substream.sender.push({ done: true });
								break;
							}
							case MSG_LOCAL_FINAL: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								substream.sender.push({ done: true, final: true });
								break;
							}
							case MSG_LOCAL_CLOSED: {
								const stream_id = await consumeUInt32LE(packeter);
								const substream = substreamsLocal[stream_id];
								if (!substream) {
									throw new Error(`No local substream for stream id ${ stream_id }`);
								}
								substreamsLocal[stream_id] = null;
								substreamsLocalFreeList.push(stream_id)
								break;
							}
							default:
								throw new Error(`Unknown packet type ${ type } received`)
						}
					}
				}
			})().catch((error) => {
				sender.push({ throw: true, error });
			});

			try {
				while (true) {
					const packet = await sender.shift();
					if (packet.done) {
						return;
					}
					if (packet.throw) {
						throw packet.error;
					}
					yield packet.value;
				}
			}
			finally {
				controller.abort();
				// No need to clear this.#pendingStreamsHandler here
				// Streams created after the parent closed would be automatically aborted
			}
		}
	}
}

export class PacketSender {
	#signal;
	#signalListener;
	#packets;
	#waiters;

	constructor(options) {
		this.#signal = options && options.signal;
		this.#packets = [];
		this.#waiters = [];
		if (this.#signal && !this.#signal.aborted) {
			this.#signalListener = () => {
				// Wake up all the waiters so they can see the signal
				const waiters = this.#waiters;
				this.#waiters = [];
				for (const waiter of waiters) {
					waiter();
				}
			};
			addWeakListener(this.#signal, 'abort', this.#signalListener, { once: true });
		}
	}

	push(packet) {
		if (this.#signal && this.#signal.aborted) {
			// Pushing is one way, so no need for any error indication here
			return;
		}
		this.#packets.push(packet);
		const waiters = this.#waiters;
		this.#waiters = [];
		for (const waiter of waiters) {
			waiter();
		}
	}

	shift() {
		return new Promise((resolve, reject) => {
			const tryGetPacket = () => {
				if (this.#signal && this.#signal.aborted) {
					reject(this.#signal.reason);
					return;
				}
				if (this.#packets.length) {
					return resolve(this.#packets.shift());
				}
				this.#waiters.push(tryGetPacket);
			}
			tryGetPacket();
		});
	}
}
