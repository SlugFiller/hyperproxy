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

import {
	Abort,
} from './abort.ts';
import {
	ChangeListener,
} from './change.ts';
import {
	createPipe,
} from './pin-stream.ts';
import type {
	PipeWritePin,
} from './pin-stream.ts';

// Runs until `main` and every process written to `processes` has settled, either succesfully or with error
// Passes to each an `Abort` that aborts if:
// - The passed `abort` has aborted
// - Any of the passed `aborts` has aborted
// - `main` settled with an error
// - Any of the processes written to `processes` has thrown
// Returns the value returned from `main` so long as none of the above conditions occurred,
// otherwise, throws the first error or reason corresponding to the above
// If `main` exits before `processes` reaches the `finished` state, it is transitioned into the `no_more` state
// `maxConcurrent`, if specified, is the maximal number of processes that may run concurrently
// No additional processes will be read from `processes` if the max concurrent processes are reached
export async function runProcesses<T>(main: (processes: PipeWritePin<(options?: {
	abort?: Abort,
}) => Promise<void>>, options?: {
	abort?: Abort,
}) => Promise<T>, options: {
	abort?: Abort,
	aborts?: (Abort | undefined)[],
	maxConcurrent?: number,
} = {}): Promise<T> {
	const {
		abort,
		aborts,
		maxConcurrent = 0,
	} = options;

	const { readPin, writePin } = createPipe<(options?: {
		abort?: Abort,
	}) => Promise<void>>();
	const mainAbort = new Abort();
	let mainFinished = false;
	const mainFinishedChange = new ChangeListener();
	let readerFinished = false;
	let runningProcesses = 0;
	const processChange = new ChangeListener();
	const maxProcessChange = new ChangeListener();

	const [ret] = await Promise.allSettled([
		(async () => {
			// Spawn main
			try {
				return await main(writePin, {
					abort: mainAbort,
				});
			}
			catch (error) {
				mainAbort.abort(error);
				throw error;
			}
			finally {
				mainFinished = true;
				mainFinishedChange.change();
				// Also need to inform the process waiter
				processChange.change();
			}
		})(),
		(async () => {
			// Read pipe and spawn processes
			try {
				while (true) {
					const listener = new ChangeListener(readPin.changeRoot);

					try {
						if (!mainFinished) {
							listener.addRoot(mainFinishedChange);
						}

						const state = readPin.state;

						if (state.state === 'idle') {
							if (mainFinished) {
								readPin.noMore();
								break;
							}
							if (maxConcurrent > 0 && runningProcesses >= maxConcurrent) {
								// Can't read any more processes
								// Wait until more room is free
								listener.addRoot(maxProcessChange);
								await listener.changed;
								continue;
							}
							readPin.wantsValue();
							continue;
						}

						if (state.state === 'has_value') {
							readPin.gotValue();
							runningProcesses++;
							state.value({
								abort: mainAbort,
							}).then(() => {
								runningProcesses--;
								if (runningProcesses < 1) {
									// Inform possible all-settled
									processChange.change();
								}
								if (maxConcurrent > 0 && runningProcesses === maxConcurrent - 1) {
									// Possible room for more processes
									maxProcessChange.change();
								}
							}).catch((error) => {
								mainAbort.abort(error);
								runningProcesses--;
								if (runningProcesses < 1) {
									// Inform possible all-settled
									processChange.change();
								}
								if (maxConcurrent > 0 && runningProcesses === maxConcurrent - 1) {
									// Possible room for more processes
									maxProcessChange.change();
								}
							});
							continue;
						}

						if (state.state !== 'wants_value') {
							if (state.state === 'finished') {
								readPin.gotFinish();
							}
							// No more processes
							break;
						}

						if (mainFinished) {
							// No writer. Write a dummy value so that we can transition to `no_more`
							writePin.setValue(() => Promise.resolve());
							continue;
						}

						await listener.changed;
					}
					finally {
						// Ensure cleanup from all change roots
						listener.change();
					}
				}
			}
			finally {
				// Done reading
				readerFinished = true;
				processChange.change();
			}
		})(),
		(async () => {
			// Wait for main and all processes to settle
			// Also handle delivering abort while processes or main are running
			let aborted = false;
			while (true) {
				const listener = new ChangeListener(processChange);

				try {
					if (!aborted) {
						if (abort) {
							listener.addRoot(abort.changeRoot);
							if (abort.aborted) {
								mainAbort.abort(abort.reason);
								// Abort was delivered, we can stop caring about it
								aborted = true;
								continue;
							}
						}
						if (aborts) {
							for (const eachAbort of aborts) {
								if (eachAbort) {
									listener.addRoot(eachAbort.changeRoot);
									if (eachAbort.aborted) {
										mainAbort.abort(eachAbort.reason);
										// Abort was delivered, we can stop caring about it
										aborted = true;
										continue;
									}
								}
							}
						}
					}

					if (readerFinished && mainFinished && runningProcesses < 1) {
						// All processes settled, and no more processes can be spawned
						break;
					}

					await listener.changed;
				}
				finally {
					// Ensure cleanup from all change roots
					listener.change();
				}
			}
		})(),
	]);

	if (mainAbort.aborted) {
		throw mainAbort.reason;
	}

	if (ret.status === 'rejected') {
		throw ret.reason;
	}
	return ret.value;
}
