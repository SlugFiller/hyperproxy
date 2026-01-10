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

interface UnraceState {
	changeRoot: ChangeListener;
	prev: UnraceState | null;
	next: UnraceState | null;
}

// Helper to prevent race conditions, by running operations in sequence
export class Unrace {
	#last: UnraceState | null;

	constructor() {
		this.#last = null;
	}

	// Runs the action in `action` and returns the value
	// Throws if `action` throws
	// If another action is currently running, delays running `action` until it completes
	// Throws if `abort` is aborted before `action` is called
	// In such a case, `action` is not called
	// `abort` has no effect after `action` has been called
	async run<T>(action: () => Promise<T>, options: {
		abort?: Abort,
	} = {}): Promise<T> {
		const {
			abort,
		} = options;

		const state: UnraceState = {
			changeRoot: new ChangeListener(),
			prev: null,
			next: null,
		};
		if (this.#last === null) {
			// Nothing running. Set action as current runner
			this.#last = state;
		}
		else {
			// Add to queue
			this.#last.next = state;
			state.prev = this.#last;
			this.#last = state;
			// Wait until running is an option
			while (true) {
				const listener = new ChangeListener(state.changeRoot);

				try {
					if (state.prev === null) {
						// Time to run
						break;
					}

					// Check for abort
					if (abort) {
						listener.addRoot(abort.changeRoot);
						if (abort.aborted) {
							// Remove from queue
							state.prev.next = state.next;
							if (state.next !== null) {
								state.next.prev = state.prev;
							}
							else {
								this.#last = state.prev;
							}
							// Won't run
							throw abort.reason;
						}
					}

					// Wait for abort or allowed to run
					await listener.changed;
				}
				finally {
					// Ensure cleanup from all change roots
					listener.change();
				}
			}
		}

		// Actually run
		try {
			return await action();
		}
		finally {
			// Remove from queue
			if (state.next !== null) {
				state.next.prev = null;
				// Allow next in turn to run
				state.next.changeRoot.change();
			}
			else {
				this.#last = null;
			}
		}
	}
}
