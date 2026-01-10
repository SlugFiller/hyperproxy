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
} from './abort.ts';
import {
	ChangeListener,
} from './change.ts';
import type {
	PipeReadPin,
	PipeWritePin,
} from './pin-stream.ts';

interface ReadableLike {
	on(event: 'readable', cb: () => void): void;
	on(event: 'end', cb: () => void): void;
	on(event: 'error', cb: (error?: unknown) => void): void;
	on(event: 'close', cb: () => void): void;
	off(event: 'readable', cb: () => void): void;
	off(event: 'end', cb: () => void): void;
	off(event: 'error', cb: (error?: unknown) => void): void;
	off(event: 'close', cb: () => void): void;
	read(): unknown;
	destroy(reason?: unknown): void;
};

interface WritableLike {
	on(event: 'drain', cb: () => void): void;
	on(event: 'finish', cb: () => void): void;
	on(event: 'error', cb: (error?: unknown) => void): void;
	on(event: 'close', cb: () => void): void;
	off(event: 'drain', cb: () => void): void;
	off(event: 'finish', cb: () => void): void;
	off(event: 'error', cb: (error?: unknown) => void): void;
	off(event: 'close', cb: () => void): void;
	write(value: unknown): boolean;
	end(): void;
	destroy(reason?: unknown): void;
};

// Reads from `readable` and writes the read packets to `output`
// Does not perform any reading until `output` is in `wants_value` state
// Throws if `abort` is aborted or if `readable` emits an error
// Destroys `readable` in case of an abort
// Transitions `output` to `finished` state when reading from `readable` ends or `readable` is destroyed without error
// Returns once `output` is either in `finished` or `no_more` states
export async function processReadable<T>(readable: ReadableLike, output: PipeWritePin<T>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
		abort,
	} = options;
	const readableChangeRoot = new ChangeListener();
	let readableHasEnd: boolean = false;
	let readableHasError: boolean = false;
	let readableError: unknown;
	let readableHasClose: boolean = false;
	function onReadable() {
		readableChangeRoot.change();
	}
	function onEnd() {
		readableHasEnd = true;
		readableChangeRoot.change();
	}
	function onError(error: unknown) {
		readableHasError = true;
		readableError = error;
		readableChangeRoot.change();
	}
	function onClose() {
		readableHasClose = true;
		readableChangeRoot.change();
	}
	// Propagate any state changes
	readable.on('readable', onReadable);
	readable.on('end', onEnd);
	readable.on('error', onError);
	readable.on('close', onClose);
	try {
		while (true) {
			const listener = new ChangeListener(output.changeRoot);
			try {
				listener.addRoot(readableChangeRoot);
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				const state = output.state;
				if (readableHasError) {
					// Forward errors from the readable regardless of the state of the output pin
					throw readableError;
				}
				if (readableHasClose && !readableHasEnd) {
					// Stream has been unexpectedly destroyed without error. Stop here
					break;
				}
				if (state.state === 'wants_value') {
					// Check if we can read from readable
					const chunk = readable.read() as T;	// We assume the readable produces T objects
					if (chunk !== null) {
						// Write the chunk to output
						output.setValue(chunk);
						continue;	// Continue reading
					}
					if (readableHasEnd) {
						// Finished reading
						output.finish();
						break;
					}
				}
				if (state.state === 'finished' || state.state === 'finished_ack') {
					// The output transitioned to a `finished` state, and we didn't do it
					throw new Error('Unexpected output state finished');
				}
				if (state.state === 'no_more' || state.state === 'no_more_ack') {
					if (state.state === 'no_more') {
						output.gotNoMore();
					}
					// Unfortunately, if the readable is part of a Duplex like a socket, there
					// is no safe way to indicate we don't want any more data without potentially
					// stopping the writable end as well
					break;
				}
				// Wait for abort, or state change in the output pin, or state change in the readable
				await listener.changed;
			}
			finally {
				// Ensure cleanup from all change roots
				listener.change();
			}
		}
	}
	finally {
		// Clean up event handlers
		readable.off('readable', onReadable);
		readable.off('end', onEnd);
		readable.off('error', onError);
		readable.off('close', onClose);
		// Destroy the readable if an abort occurred
		if (abort?.aborted) {
			// Do not pass `abort.reason` as all this does is trigger the error handler which would cause a crash
			readable.destroy();
		}
	}
}

// Reads from `input` and writes the read packets to `writable`
// Handles back-pressure from `writable`, not writing to it nor reading from `input` if the buffer is full
// Throws if `abort` is aborted or if `writable` emits an error
// Destroys `writable` in case of an abort
// Ends `writable` if `input` transitions to `finished`
// Transitions to `no_more` if `writable` is destroyed without error
// Returns once `input` is either in `finished` or `no_more` states
export async function processWritable<T>(input: PipeReadPin<T>, writable: WritableLike, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
		abort,
	} = options;
	const writableChangeRoot = new ChangeListener();
	let writableHasDrain: boolean = true;
	let writableHasFinish: boolean = false;
	let writableHasError: boolean = false;
	let writableError: unknown;
	let writableHasClose: boolean = false;
	function onDrain() {
		writableHasDrain = true;
		writableChangeRoot.change();
	}
	function onFinish() {
		writableHasFinish = true;
		writableChangeRoot.change();
	}
	function onError(error: unknown) {
		writableHasError = true;
		writableError = error;
		writableChangeRoot.change();
	}
	function onClose() {
		writableHasClose = true;
		writableChangeRoot.change();
	}
	// Propagate any state changes
	writable.on('drain', onDrain);
	writable.on('finish', onFinish);
	writable.on('error', onError);
	writable.on('close', onClose);
	try {
		while (true) {
			const listener = new ChangeListener(input.changeRoot);
			try {
				listener.addRoot(writableChangeRoot);
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				const state = input.state;
				if (writableHasError) {
					// Forward errors from the writable regardless of the state of the input pin
					throw writableError;
				}
				if (state.state === 'idle') {
					if (writableHasClose) {
						// Writable destroyed without error
						input.noMore();
						return;
					}
					// Respect back-pressure
					if (writableHasDrain) {
						// Request a value to be written to the writable
						input.wantsValue();
						continue;
					}
				}
				if (state.state === 'has_value') {
					if (writableHasClose) {
						// Writable destroyed without error
						// Discard the value, so we can transition to `no_more`
						input.gotValue();
						continue;
					}
					// Respect back-pressure
					if (writableHasDrain) {
						// Try to write the value to writable
						const value = state.value;
						input.gotValue();
						writableHasDrain = writable.write(value);
						continue;
					}
				}
				if (state.state === 'finished' || state.state === 'finished_ack') {
					// Respect back-pressure
					if (writableHasDrain) {
						if (state.state === 'finished') {
							input.gotFinish();
						}
						// End the writable stream
						writable.end();
						break;
					}
				}
				if (state.state === 'no_more' || state.state === 'no_more_ack') {
					// The input transitioned to a `no_more` state, and we didn't do it
					throw new Error('Unexpected input state no_more');
				}
				// Wait for input state change or writable state change
				await listener.changed;
			}
			finally {
				// Ensure cleanup from all change roots
				listener.change();
			}
		}
		// Wait for 'end' to be successfully written
		while (true) {
			const listener = new ChangeListener(writableChangeRoot);
			try {
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				if (writableHasError) {
					// Forward errors from the writable regardless of the state of the input pin
					throw writableError;
				}
				if (writableHasClose || writableHasFinish) {
					// Writable ended or destroyed without error
					return;
				}
				// Wait for writable state change
				await listener.changed;
			}
			finally {
				// Ensure cleanup from all change roots
				listener.change();
			}
		}
	}
	finally {
		// Clean up event handlers
		writable.off('drain', onDrain);
		writable.off('finish', onFinish);
		writable.off('error', onError);
		writable.off('close', onClose);
		// Destroy the writable if an abort occurred
		if (abort?.aborted) {
			// Do not pass `abort.reason` as all this does is trigger the error handler which would cause a crash
			writable.destroy();
		}
	}
}
