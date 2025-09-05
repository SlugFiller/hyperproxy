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
		let buffer = Buffer.alloc(0);
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
				buffer = Buffer.concat([buffer, value]);
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
			return packet.readUInt32LE();
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
	const packet = Buffer.alloc(4);
	packet.writeUInt32LE(value);
	return packet;
}
