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
 * Represents a value that is not immediately available, but eventually will be. Can be used to
 * allow an asynchronous process to produce a value, when it is guaranteed only a single value
 * may be produced, without requiring the process to stop. Represents a non-blocking lightweight
 * alternative to using a pipe that only receives a single value.
 *
 * Example usage:
 * ```typescript
 * async function process(output: Eventual<number>): string {
 * 	const val = await doAThing();
 * 	output.setValue(val * 3 + 1);
 * 	const world = await doMoreThings();
 * 	return `Hello ${ world }`;
 * }
 *
 * const valEventual = new Eventual<number>();
 *
 * process(valEventual).then((res: string) => console.log(res));
 *
 * console.log(`Got value ${ await valEventual.getValue() }`);
 * ```
 */
export class Eventual<T> {
	#status: {
		has: false,
		value: undefined,
	} | {
		has: true,
		value: T,
	};
	#changeRoot: ChangeListener;

	/**
	 * Initial state is having no value set.
	 */
	constructor() {
		this.#status = {
			has: false,
			value: undefined,
		};
		this.#changeRoot = new ChangeListener();
	}

	/**
	 * Sets the value to `value`. Subsequent calls to {@link Eventual.getValue} return this value.
	 *
	 * @param value - The value to set.
	 */
	setValue(value: T): void {
		this.#status = {
			has: true,
			value,
		};
		this.#changeRoot.change();
	}

	/**
	 * Waits for a value to be available. Returns the value once it is set with {@link Eventual.setValue},
	 * or the last set value, if {@link Eventual.setValue} was already called.
	 *
	 * @param [options] - Additional options
	 * @param [options.abort] - If aborted, throws instead of waiting for the value.
	 * @returns The last value set by {@link Eventual.setValue}.
	 */
	async getValue(options: {
		abort?: Abort,
	} = {}): Promise<T> {
		const {
			abort,
		} = options;

		while (true) {
			using listener = new ChangeListener(this.#changeRoot);

			if (abort) {
				listener.addRoot(abort.changeRoot);
				if (abort.aborted) {
					throw abort.reason;
				}
			}

			if (this.#status.has === true) {
				return this.#status.value;
			}

			await listener.changed;
		}
	}
}
