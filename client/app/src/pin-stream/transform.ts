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
 * The buffer is consistent with a prefix of a packet, but more bytes are necessary for a full packet.
 */
export interface PacketProcessResultIncomplete {
	complete: false;
}

/**
 * The start of the buffer matches the desired packet format, and contains a full packet.
 */
export interface PacketProcessResultSuccess<T> {
	complete: true;
	/**
	 * The parsed packet.
	 */
	packet: T;
	/**
	 * The number of bytes making up the packet inside the buffer.
	 * If the packet exactly ends on the buffer's last byte, this is equal to the buffer's length.
	 */
	bytesUsed: number;
}

/**
 * The result of an attempt to parse a packet from a buffer. There's no state for a failed parse,
 * since an error should be thrown in that case. There are only states for successful or partially
 * successful parsing.
 */
export type PacketProcessResult<T> = PacketProcessResultIncomplete  | PacketProcessResultSuccess<T>;

/**
 * Reads a packet from `input`, correctly handling the case where the packet spans multiple buffers.
 * On return, if the last buffer received from `input` does not end on the last byte of the packet,
 * `input` will remain in `has_value` state with the remainder of the buffer as its value.
 * Otherwise, `input` will be in the `idle` state.
 * `packetProcessor` is repeatedly passed candidate buffers for the packet so long as it returns `complete: false`.
 * It may throw if it detects the buffer cannot be a prefix of a packet.
 * In such a case the function will throw as well.
 * If it returns `complete: true`, the returned value is returned from the function.
 *
 * @param input - The readable stream from which to read a packet.
 * @param packetProcessor - A callback that receives packet candidates, and attempts to parse them into a packet.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to read a packet and throws.
 * @returns The parsed packet.
 */
export async function readPacket<T>(input: PipeReadPin<Uint8Array>, packetProcessor: (buffer: Uint8Array) => Promise<PacketProcessResult<T>>, options: {
	abort?: Abort,
} = {}): Promise<T> {
	const {
		abort,
	} = options;

	let buffer = new Uint8Array(0);

	while (true) {
		const { value } = await receiveValue(input, {
			abort,
			consumeValue: false,
			throwOnFinished: true,
		});

		// Append new data to existing buffer
		const newBuffer = new Uint8Array(buffer.length + value.length);
		newBuffer.set(buffer);
		newBuffer.set(value, buffer.length);
		buffer = newBuffer;

		// Try to process the buffer
		const result = await packetProcessor(buffer);

		if (result.complete) {
			// Packet is complete
			if (result.bytesUsed < buffer.length) {
				// There are extra bytes, keep them in the buffer for next iteration
				const remainingBuffer = buffer.subarray(result.bytesUsed);
				// Update input to have the remaining buffer
				input.setValue(remainingBuffer);
			} else {
				// No extra bytes, consume the value
				input.gotValue();
			}
			return result.packet;
		}

		// Packet is not complete yet, continue reading
		// Consume the current value since we processed it
		input.gotValue();
		continue;
	}
}

/**
 * Reads packets from `input` and writes them to `output`.
 * See {@link readPacket} for details about `packetProcessor`.
 * Throws if `input` ends mid-packet.
 * Exits cleanly if `input` ends exactly on a packet boundary.
 *
 * @param input - The readable stream from which to read packets as bytes.
 * @param output - The writable stream to which to write packets as decoded value.
 * @param packetProcessor - A callback that receives packet candidates, and attempts to parse them into a packet.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to read packets and throws.
 */
export async function decodePacketStream<T>(input: PipeReadPin<Uint8Array>, output: PipeWritePin<T>, packetProcessor: (buffer: Uint8Array) => Promise<PacketProcessResult<T>>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
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

		// Check to see if there are any bytes to decode
		const { done } = await receiveValue(input, {
			abort,
			consumeValue: false,
		});
		if (done) {
			// Input finished
			output.finish();
			break;
		}
		const packet = await readPacket(input, packetProcessor, {
			abort,
		});

		// If we successfully parsed a packet, send it
		// We can use `setValue` directly because we already guaranteed `output` is ready above
		output.setValue(packet);
	}
}

/**
 * Reads packets from `input` and writes them to `output` after being transformed by `transform`.
 *
 * @param input - The readable stream from which to read packets.
 * @param output - The writable stream to which to write transformed packets.
 * @param transform - A callback that receives a read packet and returns a transformed packet to be written.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops processing and throws.
 */
export async function transformPacketStream<I, O>(input: PipeReadPin<I>, output: PipeWritePin<O>, transform: (inputPacket: I) => Promise<O>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
		abort,
	} = options;

	while (true) {
		// Produce nothing until a read is requested
		if (!await waitUntilCanSend(output, { abort })) {
			// Output does not want more messages
			await receiveStop(input, { abort });
			break;
		}

		// Read packet to transform
		const result = await receiveValue(input, { abort, consumeValue: false });
		if (result.done) {
			// Input finished
			output.finish();
			break;
		}
		const packet = await transform(result.value);

		// Send the transformed packet
		await sendValue(output, packet, { abort, waitConsume: true });
		// Output consumed value. Report back to input
		input.gotValue();
	}
}
