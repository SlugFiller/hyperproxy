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
	sendValue,
	waitUntilCanSend,
} from './pin-stream.ts';

/**
 * Fragments buffers read from `input` that are larger than `size` into smaller
 * packets and writes them to `output`.
 *
 * @param input - Incoming stream of buffers which may be any size.
 * @param output - Outgoing stream of buffers which will have a size of `size` or smaller.
 * @param [options] - Additional options.
 * @param [options.size=65535] - The size to which buffers should be split.
 * @param [options.abort] - If aborted, stops processing and throws.
 */
export async function fragmentPackets(input: PipeReadPin<Uint8Array>, output: PipeWritePin<Uint8Array>, options: {
	size?: number,
	abort?: Abort,
} = {}): Promise<void> {
	const {
		size = 65535,
		abort,
	} = options;

	while (true) {
		// Produce nothing until a read is requested
		if (!await waitUntilCanSend(output, {
			abort,
		})) {
			// Output does not want more messages
			await receiveStop(input, {
				abort,
			});
			break;
		}

		const bufferResult = await receiveValue(input, {
			abort,
		});
		if (bufferResult.done) {
			// Input finished
			output.finish();
			break;
		}

		const buffer = bufferResult.value;

		if (buffer.length <= size) {
			// Fits in size. Write and continue
			output.setValue(buffer);
			continue;
		}

		// Fragment
		let offset = 0;
		while (buffer.length - offset > size) {
			if (!await sendValue(output, buffer.subarray(offset, offset + size), {
				abort,
			})) {
				// Output does not want more messages
				await receiveStop(input, {
					abort,
				});
				break;
			}
			offset += size;
		}

		// Send last fragment
		if (!await sendValue(output, buffer.subarray(offset), {
			abort,
		})) {
			// Output does not want more messages
			await receiveStop(input, {
				abort,
			});
			break;
		}
	}
}
