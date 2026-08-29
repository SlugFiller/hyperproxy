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
 * Possible states for a pipe.
 */
export type PipeState<T> =
	| {
		state: 'idle',
	}
	| {
		state: 'wants_value',
	}
	| {
		state: 'no_more',
	}
	| {
		state: 'no_more_ack',
	}
	| {
		state: 'has_value',
		value: T,
	}
	| {
		state: 'finished',
	}
	| {
		state: 'finished_ack',
	}
;

/**
 * An interface representing the readable side of a pipe. Represents a readable stream of data.
 */
export interface PipeReadPin<T> {
	/**
	 * Indicates the current state of the pipe.
	 *
	 * @returns The current state
	 */
	get state(): PipeState<T>;
	/**
	 * Root that changes when the state changes.
	 *
	 * @returns A {@link ChangeListener} that changes when `state` changes.
	 */
	get changeRoot(): ChangeListener;
	/**
	 * Moves from state `idle` to state `wants_value`.
	 * Throws an error if the state is anything other than `idle`.
	 */
	wantsValue(): void;
	/**
	 * Moves from state `idle` to state `no_more`.
	 * Throws an error if the state is anything other than `idle`.
	 */
	noMore(): void;
	/**
	 * Moves from state `has_value` to state `idle`.
	 * Throws an error if the state is anything other than `has_value`.
	 */
	gotValue(): void;
	/**
	 * Moves from state `finished` to state `finish_ack`.
	 * Throws an error if the state is anything other than `finished`.
	 */
	gotFinish(): void;
	/**
	 * Overwrites the current value if the current state is `has_value`.
	 * Throws an error if the state is anything other than `has_value`.
	 *
	 * @param value - The new value to be set
	 */
	setValue(value: T): void;
}

/**
 * An interface representing the writable side of a pipe. Can be used to output a stream of data.
 */
export interface PipeWritePin<T> {
	/**
	 * Indicates the current state of the pipe.
	 *
	 * @returns The current state
	 */
	get state(): PipeState<T>;
	/**
	 * Root that changes when the state changes.
	 *
	 * @returns A {@link ChangeListener} that changes when `state` changes.
	 */
	get changeRoot(): ChangeListener;
	/**
	 * Moves from state `wants_value` to state `has_value`.
	 * Throws an error if the state is anything other than `wants_value`.
	 *
	 * @param value - The value made available to the readable side of the pipe.
	 */
	setValue(value: T): void;
	/**
	 * Moves from state `wants_value` to state `finished`.
	 * Throws an error if the state is anything other than `wants_value`.
	 */
	finish(): void;
	/**
	 * Moves from state `no_more` to state `no_more_ack`.
	 * Throws an error if the state is anything other than `wants_value`.
	 */
	gotNoMore(): void;
}

/**
 * A {@link PipeReadPin} and {@link PipeWritePin} with a shared state.
 */
export interface Pipe<T> {
	/**
	 * The readable side of the pipe.
	 */
	readPin: PipeReadPin<T>;
	/**
	 * The writable side of the pipe.
	 */
	writePin: PipeWritePin<T>;
}

class PipeImplCommonState<T> {
	#state: PipeState<T>;
	#changeRoot: ChangeListener;

	constructor(initialState: PipeState<T>) {
		this.#state = initialState;
		this.#changeRoot = new ChangeListener();
	}

	get state(): PipeState<T> {
		return this.#state;
	}

	get changeRoot(): ChangeListener {
		return this.#changeRoot;
	}

	set state(newState: PipeState<T>) {
		this.#state = newState;
		this.#changeRoot.change();
	}
}

class PipeImplReadPin<T> implements PipeReadPin<T> {
	#common: PipeImplCommonState<T>;

	constructor(common: PipeImplCommonState<T>) {
		this.#common = common;
	}

	get state(): PipeState<T> {
		return this.#common.state;
	}

	get changeRoot(): ChangeListener {
		return this.#common.changeRoot;
	}

	wantsValue(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'idle') {
			throw new Error(`Cannot wantsValue() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'wants_value',
		};
	}

	noMore(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'idle') {
			throw new Error(`Cannot noMore() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'no_more',
		};
	}

	gotValue(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'has_value') {
			throw new Error(`Cannot gotValue() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'idle',
		};
	}

	gotFinish(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'finished') {
			throw new Error(`Cannot gotFinish() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'finished_ack',
		};
	}

	setValue(value: T): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'has_value') {
			throw new Error(`Cannot setValue() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'has_value',
			value,
		};
	}
}

class PipeImplWritePin<T> implements PipeWritePin<T> {
	#common: PipeImplCommonState<T>;

	constructor(common: PipeImplCommonState<T>) {
		this.#common = common;
	}

	get state(): PipeState<T> {
		return this.#common.state;
	}

	get changeRoot(): ChangeListener {
		return this.#common.changeRoot;
	}

	setValue(value: T): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'wants_value') {
			throw new Error(`Cannot setValue() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'has_value',
			value,
		};
	}

	finish(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'wants_value') {
			throw new Error(`Cannot finish() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'finished',
		};
	}

	gotNoMore(): void {
		const currentState = this.#common.state.state;
		if (currentState !== 'no_more') {
			throw new Error(`Cannot gotNoMore() in state ${ currentState }`);
		}
		this.#common.state = {
			state: 'no_more_ack',
		};
	}
}

/**
 * Creates a pipe.
 *
 * @returns A pipe with linked readable and writable ends.
 */
export function createPipe<T>(): Pipe<T> {
	const common = new PipeImplCommonState<T>({
		state: 'idle',
	});
	const readPin = new PipeImplReadPin<T>(common);
	const writePin = new PipeImplWritePin<T>(common);

	return {
		readPin,
		writePin,
	};
}

/**
 * Creates a pre-closed pipe.
 *
 * @returns A writable pipe end in `no_more` state.
 */
export function createClosedWritePin<T>(): PipeWritePin<T> {
	const common = new PipeImplCommonState<T>({
		state: 'no_more',
	});
	return new PipeImplWritePin<T>(common);
}

/**
 * Creates pre-closed pipe.
 *
 * @returns A readable pipe end in `finished` state.
 */
export function createClosedReadPin<T>(): PipeReadPin<T> {
	const common = new PipeImplCommonState<T>({
		state: 'finished',
	});
	return new PipeImplReadPin<T>(common);
}

/**
 * Helper function for waiting until `output` reaches `wants_value` state.
 * Can be used to delay producing values until they are requested.
 * Since this is async, this cannot be used in the case where a single
 * output is shared by multiple writers, as it could cause a race condition
 * between returning from this function and actually sending a value.
 * Throws if `output` is already in `finished` state.
 *
 * @param output - The writable stream to check for readiness to receive a value.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops waiting and throws.
 * @param [options.throwOnNoMore=false] - If `true`, throws if `output` is in `no_more` state.
 *                                        Otherwise, returns `false`.
 * @returns `true` if `output` is ready to receive a value. `false` if `output` is in `no_more`
 *          state, and `throwsOnNoMore` is `false`.
 */
export async function waitUntilCanSend<T>(output: PipeWritePin<T>, options: {
	abort?: Abort,
	throwOnNoMore?: boolean,
} = {}): Promise<boolean> {
	const {
		abort,
		throwOnNoMore = false,
	} = options;
	while (true) {
		using listener = new ChangeListener(output.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = output.state;

		if (state.state === 'wants_value') {
			return true;
		}

		if (state.state === 'finished' || state.state === 'finished_ack') {
			throw new Error('Attempt to write to finished output');
		}

		if (state.state === 'no_more' || state.state === 'no_more_ack') {
			if (state.state === 'no_more') {
				output.gotNoMore();
			}
			if (throwOnNoMore) {
				throw new Error('No more data desired');
			}
			return false;
		}

		// Wait for output to be ready
		await listener.changed;
	}
}

/**
 * Helper function for sending a single value to `output`.
 * Waits for `output` to reach `wants_value` state and then writes the value.
 * Throws if `output` is already in `finished` state.
 *
 * @param output - The writable stream to which to send a value.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to send the value and throws.
 * @param [options.throwOnNoMore=false] - If `true`, throws if `output` is in `no_more` state.
 *                                        Otherwise, returns `false`.
 * @param [options.waitConsume=false] - If `true`, waits after sending until `output` transitions
 *                                      away from `has_value`. Otherwise, returns immediately after
 *                                      sending the value.
 * @returns `true` if `output` received the value. `false` if `output` is in `no_more`
 *          state, and `throwsOnNoMore` is `false`.
 */
export async function sendValue<T>(output: PipeWritePin<T>, value: T, options: {
	abort?: Abort,
	throwOnNoMore?: boolean,
	waitConsume?: boolean,
} = {}): Promise<boolean> {
	const {
		abort,
		throwOnNoMore = false,
		waitConsume = false,
	} = options;
	while (true) {
		using listener = new ChangeListener(output.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = output.state;

		if (state.state === 'wants_value') {
			output.setValue(value);
			if (waitConsume) {
				break;
			}
			return true;
		}

		if (state.state === 'finished' || state.state === 'finished_ack') {
			throw new Error('Attempt to write to finished output');
		}

		if (state.state === 'no_more' || state.state === 'no_more_ack') {
			if (state.state === 'no_more') {
				output.gotNoMore();
			}
			if (throwOnNoMore) {
				throw new Error('Input rejected value');
			}
			return false;
		}

		// Wait for output to be ready
		await listener.changed;
	}
	while (true) {
		using listener = new ChangeListener(output.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = output.state;

		if (state.state !== 'has_value') {
			return true;
		}

		// Wait for value to be consumed
		await listener.changed;
	}
}

/**
 * Helper function for transitioning `output` to `finished` state.
 * Waits for `output` to reach `wants_value` state and then transitions.
 * Returns `true` if `output` is already in `finished` state.
 *
 * @param output - The writable stream to close.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to close the stream and throws.
 * @param [options.throwOnNoMore=false] - If `true`, throws if `output` is in `no_more` state.
 *                                        Otherwise, returns `false`.
 * @param [options.waitConsume=true] - If `true`, waits after closing until `output` transitions
 *                                     from `finished` to `finished_ack`. Otherwise, returns
 *                                     immediately after closing the stream.
 * @returns `true` if the transition was successful. `false` if `output` is in `no_more`
 *          state, and `throwsOnNoMore` is `false`.
 */
export async function sendFinish<T>(output: PipeWritePin<T>, options: {
	abort?: Abort,
	throwOnNoMore?: boolean,
	waitConsume?: boolean,
} = {}): Promise<boolean> {
	const {
		abort,
		throwOnNoMore = false,
		waitConsume = true,
	} = options;
	while (true) {
		using listener = new ChangeListener(output.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = output.state;

		if (state.state === 'wants_value') {
			output.finish();
			if (waitConsume) {
				break;
			}
			return true;
		}

		if (state.state === 'finished_ack') {
			return true;
		}

		if (state.state === 'finished') {
			if (waitConsume) {
				break;
			}
			return true;
		}

		if (state.state === 'no_more' || state.state === 'no_more_ack') {
			if (state.state === 'no_more') {
				output.gotNoMore();
			}
			if (throwOnNoMore) {
				throw new Error('Input rejected value');
			}
			return false;
		}

		// Wait for output to be ready
		await listener.changed;
	}
	while (true) {
		using listener = new ChangeListener(output.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = output.state;

		if (state.state !== 'finished') {
			return true;
		}

		// Wait for value to be consumed
		await listener.changed;
	}
}

/**
 * Helper function for receiving a single value from `input`.
 * Waits for `input` to reach `has_value` state and then reads the value.
 * Throws if `input` is already in `no_more` state.
 *
 * @param input - The readable stream from which to receive a value.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to receive a value and throws.
 * @param [options.throwOnFinished=false] - If `true`, throws if `input` is in `finished` state.
 *                                          Otherwise, returns `{ done: true }`.
 * @param [options.consumeValue=true] - If `true`, automatically consumes the value by transitioning
 *                                      to the `idle` state. Otherwise {@link PipeReadPin.gotValue}
 *                                      must be manually called on `input`.
 * @param [options.consumeFinish=true] - If `true`, automatically acknowledges a `finish` by transitioning
 *                                       to the `finish_ack` state. Otherwise {@link PipeReadPin.gotFinish}
 *                                       must be manually called on `input`. Applies before throwing
 *                                       if `throwOnFinished` is true.
 * @returns `{ done: false, value }` if a value has been successfully read. `{ done: true }` if `input`
 *          is in `finished` state, and `throwOnFinished` is `false`.
 */
export async function receiveValue<T>(input: PipeReadPin<T>, options: {
	abort?: Abort,
	consumeValue?: boolean,
	consumeFinish?: boolean,
	throwOnFinished: true,
}): Promise<{
	done: false,
	value: T,
}>;
export async function receiveValue<T>(input: PipeReadPin<T>, options?: {
	abort?: Abort,
	consumeValue?: boolean,
	consumeFinish?: boolean,
	throwOnFinished?: boolean,
}): Promise<{
	done: true,
	value: undefined,
} | {
	done: false,
	value: T,
}>;
export async function receiveValue<T>(input: PipeReadPin<T>, options?: {
	abort?: Abort,
	consumeValue?: boolean,
	consumeFinish?: boolean,
	throwOnFinished?: boolean,
}): Promise<{
	done: true,
	value: undefined,
} | {
	done: false,
	value: T,
}> {
	const {
		abort,
		consumeValue = true,
		consumeFinish = true,
		throwOnFinished = false,
	} = options ?? {};
	while (true) {
		using listener = new ChangeListener(input.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = input.state;

		if (state.state === 'idle') {
			input.wantsValue();
			continue;
		}

		if (state.state === 'has_value') {
			if (consumeValue) {
				input.gotValue();
			}
			return {
				done: false,
				value: state.value,
			};
		}

		if (state.state === 'no_more' || state.state === 'no_more_ack') {
			throw new Error('Attempt to read from closed input');
		}

		if (state.state === 'finished' || state.state === 'finished_ack') {
			if (consumeFinish && state.state === 'finished') {
				input.gotFinish();
			}
			if (throwOnFinished) {
				throw new Error('Unexpected end of input data');
			}
			return {
				done: true,
				value: undefined,
			};
		}

		// Wait for input to have data
		await listener.changed;
	}
}

/**
 * Helper function for transitioning `input` to `no_more` state.
 * Waits for `input` to reach `idle` state and then transitions.
 * Returns `true` if `input` is already in `no_more` state.
 *
 * @param input - The readable stream to close.
 * @param [options] - Additional options.
 * @param [options.abort] - If aborted, stops trying to close the stream and throws.
 * @param [options.throwOnFinished=false] - If `true`, throws if `input` is in `finished` state.
 *                                          Otherwise, returns `false`.
 * @param [options.consumeValue='consume'] - Behavior for when `input` is in `has_value` state. If
 *                                           `consume`, consumes and discards the value before
 *                                           transitioning to `no_more`. if `throw`, throws. If
 *                                           `retain`, stops attempting to transition to `no_more`,
 *                                           and returns `false` instead.
 * @param [options.consumeFinish=true] - If `true`, automatically acknowledges a `finish` by transitioning
 *                                       to the `finish_ack` state. Otherwise {@link PipeReadPin.gotFinish}
 *                                       must be manually called on `input`. Applies before throwing
 *                                       if `throwOnFinished` is true.
 * @param [options.waitConsume=true] - If `true`, waits after closing until `input` transitions
 *                                     from `no_more` to `no_more_ack`. Otherwise, returns
 *                                     immediately after closing the stream.
 * @returns `true` if the transition was successful. `false` if `input`
 *          is in `finished` state, and `throwOnFinished` is `false`, or `input` is in `has_value`.
 *          state and `consumeValue` is `retain`.
 */
export async function receiveStop<T>(input: PipeReadPin<T>, options: {
	abort?: Abort,
	consumeValue?: 'consume' | 'throw' | 'retain',
	consumeFinish?: boolean,
	throwOnFinished?: boolean,
	waitConsume?: boolean,
} = {}): Promise<boolean> {
	const {
		abort,
		consumeValue = 'consume',
		consumeFinish = true,
		throwOnFinished = false,
		waitConsume = true,
	} = options;
	while (true) {
		using listener = new ChangeListener(input.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = input.state;

		if (state.state === 'idle') {
			input.noMore();
			if (waitConsume) {
				break;
			}
			return true;
		}

		if (state.state === 'has_value') {
			if (consumeValue === 'consume') {
				input.gotValue();
				continue;
			}
			if (consumeValue === 'retain') {
				return false;
			}
			throw new Error('Value already received when attempting to reject');
		}

		if (state.state === 'no_more_ack') {
			return true;
		}

		if (state.state === 'no_more') {
			if (waitConsume) {
				break;
			}
			return true;
		}

		if (state.state === 'finished' || state.state === 'finished_ack') {
			if (consumeFinish && state.state === 'finished') {
				input.gotFinish();
			}
			if (throwOnFinished) {
				throw new Error('Unexpected end of input data');
			}
			return false;
		}

		// Wait for input to have data
		await listener.changed;
	}
	while (true) {
		using listener = new ChangeListener(input.changeRoot);

		if (abort) {
			listener.addRoot(abort.changeRoot);
			if (abort.aborted) {
				throw abort.reason;
			}
		}

		const state = input.state;

		if (state.state !== 'no_more') {
			return true;
		}

		// Wait for value to be consumed
		await listener.changed;
	}
}
