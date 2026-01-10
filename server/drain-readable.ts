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
} from './pin-stream/abort.ts';
import {
	ChangeListener,
} from './pin-stream/change.ts';

// Ensure compatilibity with both NodeJS and streamx
interface EndingReadable {
	on(event: 'readable', cb: () => void): void;
	on(event: 'end', cb: () => void): void;
	on(event: 'close', cb: () => void): void;
	off(event: 'readable', cb: () => void): void;
	off(event: 'end', cb: () => void): void;
	off(event: 'close', cb: () => void): void;
	read(): unknown;
	get destroyed(): boolean;
	readonly ended?: boolean;
	readonly readableEnded?: boolean;
}

// Helper to discard all remaining packets in `readable`
// If `timeout` is specified, this method throws if `readable` cannot be drained in that time
export async function drainReadable(readable: EndingReadable, options: {
	abort?: Abort,
	timeout?: number,
} = {}): Promise<void> {
	const {
		abort,
		timeout,
	} = options;

	const changeRoot = new ChangeListener();
	function onChange() {
		changeRoot.change();
	}
	readable.on('readable', onChange);
	readable.on('end', onChange);
	readable.on('close', onChange);
	let timedOut = false;
	const timeoutRef = timeout ? setTimeout(() => {
		timedOut = true;
		changeRoot.change();
	}, timeout) : null;
	try {
		while (true) {
			const listener = new ChangeListener(changeRoot);
			try {
				if (readable.destroyed || readable.ended || readable.readableEnded) {
					// Drain complete, return
					return;
				}
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				if (timedOut) {
					throw new Error('Timed out attempting to drain readable');
				}
				if (readable.read() !== null) {
					// Read and discarded a packet
					// Check status again
					continue;
				}

				// Wait for timeout, abort, drain, or next packet
				await listener.changed;
			}
			finally {
				listener.change();
			}
		}
	}
	finally {
		readable.off('readable', onChange);
		readable.off('end', onChange);
		readable.off('close', onChange);
		!timedOut && timeoutRef !== null && clearTimeout(timeoutRef);
	}
}
