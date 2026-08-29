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
	Abort,
} from './abort.ts';
import {
	ChangeListener,
} from './change.ts';

/**
 * Manages multiple processes running in parallel. Exposes `writePin`,
 * to which processes can be written.
 *
 * Example usage:
 * ```typescript
 * await using processes = new Processes({ abort: parentAbort });
 *
 * const { writePin, abort } = processes;
 *
 * sendValue(writePin, async () => {
 * 	await doSomeAction1({ abort });
 * }, { abort });
 *
 * sendValue(writePin, async () => {
 * 	await doSomeAction2({ abort });
 * }, { abort });
 *
 * await doSomeAction3({ abort });
 *
 * // doSomeAction1, doSomeAction2, doSomeAction3 all run in parallel
 *
 * // Wait for doSomeAction1 and doSomeAction2 to complete
 * await processes.finish();
 * ```
 */
export class Processes {
	#stack?: string;
	#abort: Abort;
	#changeRoot: ChangeListener;
	#runningProcesses: number;
	#runningAbortMerge: boolean;

	/**
	 * If any {@link Abort}s are provided in the options, starts a background process that
	 * delivers an abort on these to {@link Processes.abort}.
	 *
	 * @param [options] - Initialization options.
	 * @param [options.abort] - If aborted, all processes are aborted.
	 * @param [options.aborts] - All process are aborted if any of the supplied {@link Abort}s are aborted.
	 *                           Allows merging abort signals.
	 */
	constructor(options: {
		abort?: Abort,
		aborts?: (Abort | undefined)[],
	} = {}) {
		// Capture the stack here and not in `run`, because Processes has a scope-bound
		// lifecycle, while a process started with `run` is fire-and-forget and could easily
		// outlive its calling stack
		this.#stack = new Error('Thrown from process').stack;
		this.#abort = new Abort();
		this.#changeRoot = new ChangeListener();
		this.#runningProcesses = 0;
		this.#runningAbortMerge = false;
		if (options.abort || options.aborts) {
			// This process needs to be counted separately from the other process
			// so that it doesn't block the processes finishing
			this.#runningAbortMerge = true;
			this.#abortMerge(options).then(() => {
				this.#runningAbortMerge = false;
				if (this.#runningProcesses <= 0) {
					// Inform possible all-settled
					this.#changeRoot.change();
				}
			}).catch((error: unknown) => {
				// This should be unreachable. Handle it anyway
				if (!this.#abort.aborted) {
					const deliverError = new Error('Thrown abort merge', { cause: error });
					deliverError.stack = this.#stack;
					this.#abort.abort(deliverError);
				}
				this.#runningAbortMerge = false;
				if (this.#runningProcesses <= 0) {
					// Inform possible all-settled
					this.#changeRoot.change();
				}
			});
		}
	}

	/**
	 * An {@link Abort} that aborts if:
	 * - Any of the processes have thrown.
	 * - The instance of this class went out of scope.
	 * - Any of the {@link Abort}s passed to the constructor have aborted.
	 */
	get abort(): Abort {
		return this.#abort;
	}

	/**
	 * Access property to allow testing the current status of this {@link Processes} without blocking.
	 *
	 * @returns `true` if and only if no processes are running currently.
	 */
	get finished(): boolean {
		return this.#runningProcesses <= 0;
	}

	/**
	 * Starts a new process.
	 *
	 * @param process - The process to run. Runs in parallel with any existing processes
	 */
	run(process: () => Promise<void>): void {
		if (this.#abort.aborted) {
			throw this.#abort.reason;
		}
		this.#runningProcesses++;
		process().then(() => {
			this.#runningProcesses--;
			if (this.#runningProcesses <= 0) {
				// Inform possible all-settled
				this.#changeRoot.change();
			}
		}).catch((error: unknown) => {
			if (!this.#abort.aborted) {
				const deliverError = new Error('Thrown from process', { cause: error });
				deliverError.stack = this.#stack;
				this.#abort.abort(deliverError);
			}
			this.#runningProcesses--;
			if (this.#runningProcesses <= 0) {
				// Inform possible all-settled
				this.#changeRoot.change();
			}
		});
	}

	/**
	 * Waits for all currently running processes to complete.
	 * It is still possible to use this instance after this function returns.
	 *
	 * Throws without waiting for the processes if:
	 * - Any of the processes have thrown.
	 * - `abort` is aborted.
	 * - Any of the {@link Abort}s passed to the constructor have aborted.
	 * - The instance of this class went out of scope. Normally, this method should be called from
	 *   inside the same scope.
	 *
	 * @param [options] - Additional options.
	 * @param [options.abort] - If aborted, stops waiting for all processes to finish and throws.
	 */
	async finish(options: {
		abort?: Abort,
	} = {}): Promise<void> {
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

			listener.addRoot(this.#abort.changeRoot);
			if (this.#abort.aborted) {
				throw this.#abort.reason;
			}

			if (this.#runningProcesses <= 0) {
				break;
			}

			await listener.changed;
		}
	}

	/**
	 * Enables using this class in `using` syntax. Aborts any still running processes, and waits
	 * for all processes to finish. If any process ended in error before or after reaching this
	 * point, that error will be discarded.
	 */
	async [Symbol.asyncDispose](): Promise<void> {
		this.#abort.abort(new Error('Processes left scope'));
		while (true) {
			using listener = new ChangeListener(this.#changeRoot);
			if (!this.#runningAbortMerge && this.#runningProcesses <= 0) {
				break;
			}
			await listener.changed;
		}
	}

	async #abortMerge(options: {
		abort?: Abort,
		aborts?: (Abort | undefined)[],
	} = {}): Promise<void> {
		const {
			abort,
			aborts,
		} = options;

		// Allow other aborts to be delivered to the main abort
		while (true) {
			using listener = new ChangeListener(this.#changeRoot);

			listener.addRoot(this.#abort.changeRoot);

			if (!this.#abort.aborted && abort) {
				listener.addRoot(abort.changeRoot);
				if (abort.aborted) {
					this.#abort.abort(abort.reason);
				}
			}
			if (!this.#abort.aborted && aborts) {
				for (const eachAbort of aborts) {
					if (eachAbort) {
						listener.addRoot(eachAbort.changeRoot);
						if (eachAbort.aborted) {
							this.#abort.abort(eachAbort.reason);
							break;
						}
					}
				}
			}

			if (this.#abort.aborted) {
				break;
			}

			await listener.changed;
		}
	}
}
