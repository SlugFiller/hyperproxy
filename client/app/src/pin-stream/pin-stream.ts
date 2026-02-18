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

export type PipeState<T> = {
	state: 'idle',
} | {
	state: 'wants_value',
} | {
	state: 'no_more',
} | {
	state: 'no_more_ack',
} | {
	state: 'has_value',
	value: T,
} | {
	state: 'finished',
} | {
	state: 'finished_ack',
};

export interface PipeReadPin<T> {
	// Indicates the current state of the pipe
	get state(): PipeState<T>;
	// Root that changes when the state changes
	get changeRoot(): ChangeListener;
	// Moves from state `idle` to state `wants_value`
	// Throws an error if the state is anything other than `idle`
	wantsValue(): void;
	// Moves from state `idle` to state `no_more`
	// Throws an error if the state is anything other than `idle`
	noMore(): void;
	// Moves from state `has_value` to state `idle`
	// Throws an error if the state is anything other than `has_value`
	gotValue(): void;
	// Moves from state `finished` to state `finish_ack`
	// Throws an error if the state is anything other than `finished`
	gotFinish(): void;
	// Overwrites the current value if the current state is `has_value`
	// Throws an error if the state is anything other than `has_value`
	setValue(value: T): void;
}

export interface PipeWritePin<T> {
	// Indicates the current state of the pipe
	get state(): PipeState<T>;
	// Root that changes when the state changes
	get changeRoot(): ChangeListener;
	// Moves from state `wants_value` to state `has_value`
	// Throws an error if the state is anything other than `wants_value`
	setValue(value: T): void;
	// Moves from state `wants_value` to state `finished`
	// Throws an error if the state is anything other than `wants_value`
	finish(): void;
	// Moves from state `no_more` to state `no_more_ack`
	// Throws an error if the state is anything other than `wants_value`
	gotNoMore(): void;
}

// A `PipeReadPin` and `PipeWritePin` with a shared state
export interface Pipe<T> {
	readPin: PipeReadPin<T>;
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

// Creates a pipe
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

// Create pre-closesd pipes
export function createClosedWritePin<T>(): PipeWritePin<T> {
	const common = new PipeImplCommonState<T>({
		state: 'no_more',
	});
	return new PipeImplWritePin<T>(common);
}

export function createClosedReadPin<T>(): PipeReadPin<T> {
	const common = new PipeImplCommonState<T>({
		state: 'finished',
	});
	return new PipeImplReadPin<T>(common);
}

// Helper function for waiting until `output` reaches `wants_value` state
// Can be used to delay producing values until they are requested
// Since this is async, this cannot be used in the case where a single
// output is shared by multiple writers, as it would cause a race condition
// Returns `true` if the output is ready to receive a value
// If `output` is in `no_more` state, if `throwOnNoMore` is `true` then throws, otherwise returns `false`
// Throws if `output` is already in `finished` state
// Throws if `abort` is aborted
export async function waitUntilCanSend<T>(output: PipeWritePin<T>, options: {
	abort?: Abort,
	throwOnNoMore?: boolean,
} = {}): Promise<boolean> {
	const {
		abort,
		throwOnNoMore = false,
	} = options;
	while (true) {
		const listener = new ChangeListener(output.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
}

// Helper function for sending a single value to `output`
// Waits for `output` to reach `wants_value` state and then writes the value
// Returns `true` if the value has been written
// If `output` is in `no_more` state, if `throwOnNoMore` is `true` then throws, otherwise returns `false`
// If `waitConsume` is `true`, waits after sending until `output` transitions away from `has_value`
// Throws if `output` is already in `finished` state
// Throws if `abort` is aborted
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
		const listener = new ChangeListener(output.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
	while (true) {
		const listener = new ChangeListener(output.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
}

// Helper function for transitioning `output` to `finished` state
// Waits for `output` to reach `wants_value` state and then transitions
// Returns `true` if the transition was successful
// If `output` is in `no_more` state, if `throwOnNoMore` is `true` then throws, otherwise returns `false`
// If `waitConsume` is not `false`, waits after finishing until `output` transitions away from `finished`
// Returns `true` if `output` is already in `finished` state
// Throws if `abort` is aborted
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
		const listener = new ChangeListener(output.changeRoot);

		try {
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

			if (state.state === 'finished' || state.state === 'finished_ack') {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
	while (true) {
		const listener = new ChangeListener(output.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
}

// Helper function for receiving a single value from `input`
// Waits for `input` to reach `has_value` state and then reads the value
// Returns `{ done: false, value }` if a value has been successfully read
// Automatically consumes the value if `consumeValue` is not `false`. Otherwise `input.gotValue` must be manually called
// Automatically acknowledges a finish if `consumeFinish` is not `false`. Otherwise `input.gotFinish` must be manually called
// If `input` is in `finished` state, if `throwOnFinished` is `true` then throws, otherwise returns `{ done: true }`
// Throws if `input` is already in `no_more` state
// Throws if `abort` is aborted
export async function receiveValue<T, const O extends {
	abort?: Abort,
	consumeValue?: boolean,
	consumeFinish?: boolean,
	throwOnFinished?: boolean,
}>(input: PipeReadPin<T>, options?: O): Promise<O extends { throwOnFinished: true } ? {
	done: false,
	value: T,
} : ({
	done: true,
	value: undefined,
} | {
	done: false,
	value: T,
})> {
	const {
		abort,
		consumeValue = true,
		consumeFinish = true,
		throwOnFinished = false,
	} = options ?? {};
	while (true) {
		const listener = new ChangeListener(input.changeRoot);

		try {
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
				// Unfortunately, TypeScript cannot figure out the type narrowing here, so we help it
				return {
					done: true,
					value: undefined,
				} as unknown as Promise<O extends { throwOnFinished: true } ? {
					done: false,
					value: T,
				} : ({
					done: true,
					value: undefined,
				} | {
					done: false,
					value: T,
				})>;
			}

			// Wait for input to have data
			await listener.changed;
		}
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
}

// Helper function for transitioning `input` to `no_more` state
// Waits for `input` to reach `idle` state and then transitions
// Returns `true` if the transition was successful
// If `input` is in `has_value` state, consumes the value if `consumeValue` is `consume` (default), throws if `consumeValue` is `throw`. otherwise returns `false`
// Automatically acknowledges a finish if `consumeFinish` is not `false`. Otherwise `input.gotFinish` must be manually called
// If `waitConsume` is not `false`, waits after stopping until `input` transitions away from `no_more`
// If `input` is in `finished` state, if `throwOnFinished` is `true` then throws, otherwise returns `false`
// Retuns true `input` is already in `no_more` state
// Throws if `abort` is aborted
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
		const listener = new ChangeListener(input.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
	while (true) {
		const listener = new ChangeListener(input.changeRoot);

		try {
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
		finally {
			// Ensure cleanup from all change roots
			listener.change();
		}
	}
}
