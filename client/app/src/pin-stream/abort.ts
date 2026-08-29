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
	ChangeListener,
} from './change.ts';

/**
 * An instance of this class is passed to a process in order to make it abortable. This
 * class is intended to work in combination with {@link ChangeListener}.
 *
 * Example usage:
 * ```typescript
 * async function process(options?: {
 * 	abort?: Abort,
 * }): Promise<string> {
 * 	const {
 * 		abort,
 * 	} = options;
 * 	while (true) {
 * 		// Listen for state changes
 * 		using listener = new ChangeListener(changeRoot);
 *
 * 		// If an abort has been passed (Could be undefined)
 * 		if (abort) {
 * 			// Listen to a future abort, if not yet aborted
 * 			listener.addRoot(abort.changeRoot);
 * 			// After listening, check if already aborted
 * 			if (abort.aborted) {
 * 				// Handle abort by throwing. Depending on the process, some aborts
 * 				// can be handled quietly without error. But an exception is the
 * 				// most common outcome.
 * 				throw abort.reason;
 * 			}
 * 		}
 *
 * 		// Check the state, and act accordingly
 * 		const state = currentState;
 * 		if (needsAction(state)) {
 * 			doAction();
 * 			continue;
 * 		}
 *
 * 		if (hasResult(state)) {
 * 			return getResult(state);
 * 		}
 *
 * 		// Wait for either abort or a state change
 * 		await listener.changed;
 * 	}
 * }
 * ```
 */
export class Abort {
	#aborted: boolean;
	#reason: unknown;
	#changeRoot: ChangeListener;

	constructor() {
		this.#aborted = false;
		this.#reason = undefined;
		this.#changeRoot = new ChangeListener();
	}

	/**
	 * Indicates whether {@link Abort.abort} has already been called on this {@link Abort}.
	 *
	 * @returns `true` if {@link Abort.abort} has been called, otherwise `false`.
	 */
	get aborted(): boolean {
		return this.#aborted;
	}

	/**
	 * If {@link Abort.abort} has been called, returns the value of the `reason` parameter, otherwise `undefined`.
	 *
	 * @returns The value of the `reason` parameter in the first call to {@link Abort.abort}.
	 */
	get reason(): unknown {
		return this.#reason;
	}

	/**
	 * A change root that can be used to monitor when the value of {@link Abort.aborted} changes.
	 *
	 * @returns A change root that changes the first time {@link Abort.abort} is called.
	 */
	get changeRoot(): ChangeListener {
		return this.#changeRoot;
	}

	/**
	 * If called for the first time, sets `aborted` to `true`, `reason` to the `reason` parameter,
	 * and call `change` on `changeRoot`.
	 * From the second call onward, does nothing.
	 *
	 * @param [reason] - An arbitrary value, to be returned by {@link Abort.reason}.
	 */
	abort(reason?: unknown): void {
		if (this.#aborted) {
			return;
		}
		this.#aborted = true;
		this.#reason = reason;
		this.#changeRoot.change();
	}
}

/**
 * Convenience function to wait until at least one of the supplied `aborts` has aborted.
 *
 * @param aborts - A list of {@link Abort}s. The function returns once any of them has aborted.
 */
export async function anyAbort(...aborts: (Abort | undefined)[]): Promise<void> {
	let firstAbort: Abort | undefined;
	for (const abort of aborts) {
		if (abort) {
			firstAbort = abort;
			break;
		}
	}
	if (!firstAbort) {
		// Not a single valid abort in the list
		// Prefer a return over freezing the program
		return;
	}
	while (true) {
		using listener = new ChangeListener(firstAbort.changeRoot);

		for (const abort of aborts) {
			if (abort) {
				listener.addRoot(abort.changeRoot);
				if (abort.aborted) {
					return;
				}
			}
		}

		await listener.changed;
	}
}
