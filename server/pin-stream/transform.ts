import type {
	Abort,
} from './abort.ts';
import {
	receiveStop,
	receiveValue,
	waitUntilCanSend,
} from './pin-stream.ts';
import type {
	PipeReadPin,
	PipeWritePin,
} from './pin-stream.ts';

// Type for processing a packet from a buffer
export type PacketProcessResult<T> = {
	// The buffer is consistent with a prefix of a packet, but more bytes are necessary for a full packet
	complete: false,
} | {
	// The start of the buffer matches the desired packet format, and contains a full packet
	complete: true,
	packet: T,
	// The number of bytes making up the packet inside the buffer
	// If the packet exactly ends on the buffer's last byte, this is equal to the buffer's length
	bytesUsed: number,
};

// Reads a packet from `input`, correctly handling the case where the packet spans multiple buffers
// Throws if `abort` is aborted during processing
// On return, if the last buffer received from `input` does not end on the last byte of the packet,
// `input` will remain in `has_value` state with the remainder of the buffer as its value
// Otherwise, `input` will be in the `idle` state
// `packetProcessor` is repeatedly passed candidate buffers for the packet so long as it returns `complete: false`
// It may throw if it detects the buffer cannot be a prefix of a packet
// In such a case the function will throw as well
// If it returns `complete: true`, the returned value is returned from the function
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
		input.gotValue(); // Consume the current value since we processed it
		continue;
	}
}

// Reads packets from `input` and writes them to `output`
// See `readPacket` for details about `packetProcessor`
// Throws if `input` ends mid-packet
// Throws if `abort` is aborted during processing
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
			return;
		}
		const packet = await readPacket(input, packetProcessor, {
			abort,
		});

		// If we successfully parsed a packet, send it
		// We can use `setValue` directly because we already guaranteed `output` is ready above
		output.setValue(packet);
	}
}

// Reads packets from `input` and writes them to `output` after being transformed by `transform`
// Throws if `abort` is aborted during processing
export async function transformPacketStream<I, O>(input: PipeReadPin<I>, output: PipeWritePin<O>, transform: (inputPacket: I) => Promise<O>, options?: {
	abort?: Abort,
}): Promise<void> {
	while (true) {
		// Produce nothing until a read is requested
		if (!await waitUntilCanSend(output, options)) {
			// Output does not want more messages
			await receiveStop(input, options);
			break;
		}

		// Read packet to transform
		const result = await receiveValue(input, options);
		if (result.done) {
			// Input finished
			output.finish();
			return;
		}
		const packet = await transform(result.value);

		// Send the transformed packet
		// We can use `setValue` directly because we already guaranteed `output` is ready above
		output.setValue(packet);
	}
}
