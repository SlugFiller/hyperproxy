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

// Waits for `millis` milliseconds
// Throws immediately if `abort` aborts while waiting
export async function waitTimeout(millis: number, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
		abort,
	} = options;
	const changeRoot = new ChangeListener();
	let timedOut = false;
	const timeoutRef = setTimeout(() => {
		timedOut = true;
		// Signal to the listener that a timeout has occurred
		changeRoot.change();
	}, millis);
	try {
		while (true) {
			const listener = new ChangeListener(changeRoot);
			try {
				// Check for abort
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				// Check for timeout
				if (timedOut) {
					return;
				}

				// Wait for timeout or abort
				await listener.changed;
			}
			finally {
				listener.change();
			}
		}
	}
	finally {
		!timedOut && clearTimeout(timeoutRef);
	}
}
