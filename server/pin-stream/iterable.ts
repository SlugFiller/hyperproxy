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
} from './abort.ts';
import {
	type PipeReadPin,
	type PipeWritePin,
	receiveStop,
	receiveValue,
	sendFinish,
	sendValue,
} from './pin-stream.ts';

/**
 * Feeds data from an iterator to a stream.
 *
 * @param input - Input iterator from which data is read.
 * @param output - Output stream to which data is written.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops processing and throws.
 * @param [options.sendFinish='no_error'] - If `always`, the stream is closed when iteration ends.
 *                                          If `no_error` the stream is not closed if an error has
 *                                          occurred. If `never`, the stream is left open after
 *                                          iteration ends, and may continue to receive more data.
 */
export async function fromIterable<T>(input: Iterable<T>, output: PipeWritePin<T>, options: {
	abort?: Abort,
	sendFinish?: 'always' | 'no_error' | 'never';
} = {}): Promise<void> {
	const {
		abort,
		sendFinish: shouldSendFinish = 'no_error',
	} = options;

	try {
		for (const element of input) {
			if (!await sendValue(output, element, { abort })) {
				break;
			}
		}
		if (shouldSendFinish === 'no_error') {
			await sendFinish(output, { abort });
		}
	}
	finally {
		if (shouldSendFinish === 'always') {
			await sendFinish(output, { abort });
		}
	}
}

/**
 * Feeds data from an async iterator to a stream.
 *
 * @param input - Input iterator from which data is read.
 * @param output - Output stream to which data is written.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops processing and throws.
 * @param [options.sendFinish='no_error'] - If `always`, the stream is closed when iteration ends.
 *                                          If `no_error` the stream is not closed if an error has
 *                                          occurred. If `never`, the stream is left open after
 *                                          iteration ends, and may continue to receive more data.
 */
export async function fromAsyncIterable<T>(input: AsyncIterable<T>, output: PipeWritePin<T>, options: {
	abort?: Abort,
	sendFinish?: 'always' | 'no_error' | 'never',
} = {}): Promise<void> {
	const {
		abort,
		sendFinish: shouldSendFinish = 'no_error',
	} = options;

	try {
		for await (const element of input) {
			if (!await sendValue(output, element, { abort })) {
				break;
			}
		}
		if (shouldSendFinish === 'no_error') {
			await sendFinish(output, { abort });
		}
	}
	finally {
		if (shouldSendFinish === 'always') {
			await sendFinish(output, { abort });
		}
	}
}

/**
 * Iterates over data from a stream.
 *
 * @param input - Input stream from which data is read.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops processing and throws.
 * @param [options.sendStop='always'] - If `always`, the stream is closed if iteration ends
 *                                      prematurely. If `no_error` the stream is only closed if
 *                                      breaking out of a loop, but not if an error occurred.
 *                                      If `never`, the stream is left open if iteration ends
 *                                      prematurely, and data can continue to be read.
 */
export async function* toAsyncIterable<T>(input: PipeReadPin<T>, options: {
	abort?: Abort,
	sendStop?: 'always' | 'no_error' | 'never',
} = {}): AsyncGenerator<T, void, void> {
	const {
		abort,
		sendStop = 'always',
	} = options;

	let needAck = false;
	let state: 'run' | 'done' | 'error' = 'run';
	try {
		while (true) {
			const { done, value } = await receiveValue(input, { abort, consumeValue: false });
			if (done) {
				state = 'done'
				break;
			}
			needAck = true;
			yield value;
			needAck = false;
			// Only acknowledge receiving the value after returning from yield
			input.gotValue();
		}
	}
	catch (error: unknown) {
		state = 'error';
		throw error;
	}
	finally {
		if (needAck) {
			// If yield did not return normally, the input still needs to be acknowledged
			input.gotValue();
		}
		if ((state === 'run' && sendStop !== 'never') || (state === 'error' && sendStop === 'always')) {
			await receiveStop(input, { abort });
		}
	}
}
