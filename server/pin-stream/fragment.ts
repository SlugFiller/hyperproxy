import type {
	Abort,
} from './abort.ts';
import {
	receiveStop,
	receiveValue,
	sendValue,
	waitUntilCanSend,
} from './pin-stream.ts';
import type {
	PipeReadPin,
	PipeWritePin,
} from './pin-stream.ts';

// Fragments buffers read from `input` that are larger than `size` into smaller
// packets and writes them to `output`
// Throws if `abort` is aborted
// `size` is 65535 by default
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
