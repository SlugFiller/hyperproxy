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

interface UnraceState {
	changeRoot: ChangeListener;
	running: boolean;
	prev: UnraceState | null;
	next: UnraceState | null;
}

/**
 * Helper to prevent race conditions, by running operations in sequence.
 *
 * Example usage:
 * ```typescript
 * const unrace = new Unrace();
 * await Promise.all([
 * 	(async () => {
 * 		using _ = await unrace.run();
 * 		await doSomething1();
 * 	})(),
 * 	(async () => {
 * 		using _ = await unrace.run();
 * 		await doSomething1();
 * 	})(),
 * ]);
 * // doSomething1 and doSomething2 will run in sequence and not in parallel
 * // However, it is undetermined which will run first
 * ```
 */
export class Unrace<K = undefined> {
	#queue: Map<K, [UnraceState, UnraceState]>;
	#active: Map<K, number>;
	#maxConcurrent: number;

	/**
	 * Initial state. No operations running.
	 *
	 * @param [maxConcurrent=1] - The number of operations that are allowed to run in parallel.
	 *                            If specified, operations do not run in sequence, but rather,
	 *                            the number of parallel operations is limited.
	 */
	constructor(maxConcurrent: number = 1) {
		this.#queue = new Map<K, [UnraceState, UnraceState]>();
		this.#active = new Map<K, number>();
		this.#maxConcurrent = maxConcurrent;
	}

	/**
	 * Waits until any prior operation finished running, or were aborted while waiting.
	 * Once this method returns, the current operation is considered "running" until the
	 * returned disposable goes out of scope.
	 *
	 * @param key - The key for which the operation count is limited. Each key has its own
	 *              independent pool of `maxConcurrent` operations.
	 * @param [options] - Additional options.
	 * @param [options.abort] - If aborted, stops waiting for prior actions to complete and throws.
	 *                          `actions` is not called in such a case. Has no effect if prior
	 *                          actions were already complete and `action` was called, even if
	 *                          it did not yet complete.
	 * @returns A disposable that, once it goes out of scope, indicates this operation is finished
	 *          and the next operation may be started.
	 */
	async run(key: K, options: {
		abort?: Abort,
	} = {}): Promise<Disposable> {
		const {
			abort,
		} = options;

		const currentActive = this.#active.get(key) ?? 0;
		const state: UnraceState = {
			changeRoot: new ChangeListener(),
			running: currentActive < this.#maxConcurrent,
			prev: null,
			next: null,
		};
		if (state.running) {
			this.#active.set(key, currentActive + 1)
		}
		else {
			// Add to queue
			const appendQueue = this.#queue.get(key);
			if (appendQueue) {
				appendQueue[1].next = state;
				state.prev = appendQueue[1];
				appendQueue[1] = state;
			}
			else {
				this.#queue.set(key, [state, state]);
			}
			// Wait until running is an option
			while (true) {
				using listener = new ChangeListener(state.changeRoot);

				if (state.running) {
					// Time to run
					break;
				}

				// Check for abort
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						// Remove from queue
						const removeQueue = this.#queue.get(key);
						if (removeQueue) {
							if (state.prev === null) {
								if (state.next === null) {
									this.#queue.delete(key);
								}
								else {
									state.next.prev = null;
									removeQueue[0] = state.next;
								}
							}
							else {
								if (state.next === null) {
									state.prev.next = null;
									removeQueue[1] = state.prev;
								}
								else {
									state.prev.next = state.next;
									state.next.prev = state.prev;
								}
							}
						}
						// Won't run
						throw abort.reason;
					}
				}

				// Wait for abort or allowed to run
				await listener.changed;
			}
		}

		// Actually run
		return {
			[Symbol.dispose]: () => {
				const popQueue = this.#queue.get(key);
				if (!popQueue) {
					// Queue empty. Just leave room
					const popActive = this.#active.get(key);
					if (popActive && popActive > 1) {
						this.#active.set(key, popActive - 1);
					}
					else {
						this.#active.delete(key);
					}
					return;
				}
				const first = popQueue[0];
				// Remove first from queue
				if (first.next === null) {
					this.#queue.delete(key);
				}
				else {
					popQueue[0] = first.next;
					first.next.prev = null;
				}
				// And allow it to run
				first.running = true;
				first.changeRoot.change();
			},
		};
	}
}
