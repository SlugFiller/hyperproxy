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
	ChangeListener,
} from './change.ts';

/**
 * A class which measures a specific time out from the moment of creation, while exposing
 * a {@link ChangeListener} that can be used to monitor when the time has elapsed.
 *
 * Example usage:
 * ```typescript
 * // Start counting 10 seconds from this moment
 * using timeout = new Timeout(10000);
 *
 * while (true) {
 * 	using listener = new ChangeListener(timeout.changeRoot);
 *
 * 	// Check for timeout
 * 	if (timeout.elapsed) {
 * 		return;
 * 	}
 *
 * 	// ...Possibly process state from other objects...
 *
 * 	// Wait for timeout
 * 	await listener.changed;
 * }
 * ```
 */
export class Timeout {
	#changeRoot: ChangeListener;
	#elapsed: boolean;
	#timeoutRef: ReturnType<typeof setTimeout>;

	constructor(millis: number) {
		this.#changeRoot = new ChangeListener();
		this.#elapsed = false;
		this.#timeoutRef = setTimeout(() => {
			this.#elapsed = true;
			this.#changeRoot.change();
		}, millis);
	}

	/**
	 * Indicates whether the time has elapsed.
	 *
	 * @returns `true` if the time specified when creating this {@link Timeout}
	 *          has already elapsed, otherwise `false`.
	 */
	get elapsed(): boolean {
		return this.#elapsed;
	}

	/**
	 * A change root that can be used to monitor when the value of {@link Timeout.elapsed} changes.
	 *
	 * @returns A change root that changes once the time has elapsed.
	 */
	get changeRoot(): ChangeListener {
		return this.#changeRoot;
	}

	/**
	 * Enables use in `using` statement. Causes this timeout to stop counting.
	 */
	[Symbol.dispose](): void {
		if (!this.#elapsed) {
			clearTimeout(this.#timeoutRef);
		}
	}
}

/**
 * Waits for `millis` milliseconds.
 *
 * @param millis - The amount of time to wait in milliseconds.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops waiting immediately and throws if `throwOnAbort` is `true`.
 * @param [options.throwOnAbort=true] - If `true`, throws if `abort` is aborted before the time has
 *                                      elapsed. Otherwise, returns normally.
 */
export async function waitTimeout(millis: number, options: {
	abort?: Abort,
	throwOnAbort?: boolean,
} = {}): Promise<void> {
	const {
		abort,
		throwOnAbort = true,
	} = options;

	using timeout = new Timeout(millis);

	while (true) {
		using listener = new ChangeListener(timeout.changeRoot);
		// Check for abort
		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				if (throwOnAbort) {
					throw abort.reason;
				}
				return;
			}
		}
		// Check for timeout
		if (timeout.elapsed) {
			return;
		}

		// Wait for timeout or abort
		await listener.changed;
	}
}
