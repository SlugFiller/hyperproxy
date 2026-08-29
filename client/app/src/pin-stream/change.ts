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

/**
 * Class for creating waitable dynamic state.
 *
 * This class is only used for managing the wait, while state should be provided separately.
 * Waiting is done "spinloop-style". First, a listener is registered. Then, the current state
 * is checked. If it is not a usable state, the listener is used to wait until the state
 * changes. After that, loop and repeat the process from the start, until the desired state is
 * reached.
 *
 * Example usage:
 * ```typescript
 * let state1: StateType1 = new StateType1();
 * const changeRoot1 = new ChangeListener();
 * let state2: StateType2 = new StateType2();
 * const changeRoot2 = new ChangeListener();
 *
 * asyncProcessThatChangesState1(state1, changeRoot1).catch((error) => {
 * 	console.log(error);
 * });
 * asyncProcessThatChangesState2(state2, changeRoot2).catch((error) => {
 * 	console.log(error);
 * });
 *
 * while (true) {
 * 	// Listen to changes to state1
 * 	using listener = new ChangeListener(changeRoot1);
 *
 * 	// This should preferably be an immutable snapshot, but that's not a must
 * 	const state1value = state1.getCurrentValue();
 * 	// Use state1's current value
 * 	if (state1value.hasReasonToStop()) {
 * 		break;
 * 	}
 * 	if (state1value.hasReasonToDoAction()) {
 * 		state1.doAction();
 * 		// state1 has likely changed as a result of the action
 * 		// Go back to the start of the loop
 * 		continue;
 * 	}
 * 	if (!state1value.hasReasonToCareAboutState2()) {
 * 		// Wait for state1 to change to a new value
 * 		await listener.changed;
 * 		continue;
 * 	}
 *
 * 	// Listen to changes in state2 too
 * 	listener.addRoot(changeRoot2);
 * 	const state2value = state2.getCurrentValue();
 * 	if (state2value.hasReasonToDoAction()) {
 * 		state2.doAction();
 * 		// state2 has likely changed as a result of the action
 * 		// state1 may have changed too
 * 		// Go back to the start of the loop
 * 		continue;
 * 	}
 *
 * 	// Wait for state1 or state2 to change
 * 	await listener.changed;
 * }
 * ```
 */
export class ChangeListener {
	#changed: boolean;
	#root: ChangeListener;
	#rootLeft: ChangeListener;
	#rootRight: ChangeListener;
	#sibling: ChangeListener;
	#promise: Promise<void> | null;
	#resolve: ((value: void | PromiseLike<void>) => void) | null;

	/**
	 * Create a {@link ChangeListener} that goes into a changed state if {@link ChangeListener.change}
	 * is called on `root`.
	 * If `root` is not specified, then this is a root.
	 * A root does not keep changed state. Instead, it only propagates changes to other
	 * {@link ChangeListener}s that have it as root.
	 * Once a {@link ChangeListener} goes into changed state, it never goes back and cannot be reused.
	 * If `root` is not a root, its root is copied instead.
	 *
	 * @param [root] - The root {@link ChangeListener} to which this {@link ChangeListener} should listen.
	 */
	constructor(root?: ChangeListener) {
		this.#changed = false;
		this.#promise = null;
		this.#resolve = null;
		this.#root = root ?? this;
		this.#sibling = this;
		if (this.#root === this) {
			this.#rootLeft = this.#rootRight = this;
		}
		else {
			// Ensure our root is a root
			this.#root = this.#root.#root;
			// Add to root
			this.#rootLeft = this.#root;
			this.#rootRight = this.#root.#rootRight;
			this.#rootLeft.#rootRight = this.#rootRight.#rootLeft = this;
		}
	}

	/**
	 * Adds an additional root to this listener.
	 * If {@link ChangeListener.change} is called on any root the listener transitions to a changes state.
	 * If `root` is not a root, its root is used instead.
	 *
	 * @param root - An additional root {@link ChangeListener} to which this {@link ChangeListener} should listen.
	 */
	addRoot(root: ChangeListener): void {
		if (this.#changed) {
			// Already in changed state. No point in waiting for another root
			return;
		}
		if (this.#root === this) {
			throw new Error('Root may not have roots added to it')
		}
		const sibling = new ChangeListener(root);
		// Add to cyclical chain as a sibling
		sibling.#sibling = this.#sibling;
		this.#sibling = sibling;
	}

	/**
	 * A promise that resolves when this {@link ChangeListener} is in a changed state.
	 * It is an error to call this on a root.
	 *
	 * @returns A promise that resolves when this {@link ChangeListener} is in a changed state.
	 */
	get changed(): Promise<void> {
		if (this.#changed) {
			return Promise.resolve();
		}
		if (this.#root === this) {
			throw new Error('Cannot directly wait for root change. Create a child instead')
		}
		if (this.#promise === null) {
			this.#promise = new Promise((resolve) => {
				this.#resolve = resolve;
			});
		}
		return this.#promise;
	}

	/**
	 * If this is a root, set all current children and their siblings to changed.
	 * If this is a non-root, set itself and all siblings to changed.
	 * For a non-root, it also performs cleanup.
	 * Either {@link ChangeListener.change} or {@link ChangeListener.changed} must be used at least
	 * once on at least one sibling to prevent leak.
	 */
	change(): void {
		if (this.#changed) {
			// Already marked as changed. No action needed
			return;
		}
		if (this.#root === this) {
			if (this.#rootRight === this) {
				// No children to signal
				return;
			}
			const start = this.#rootRight;
			let last = ChangeListener.#markAndRemoveAllSiblings(start);
			// Mark and detach children until none are left
			while (this.#rootRight !== this) {
				// Continue chain from end of current chain
				last.#root = this.#rootRight;
				// Mark additional siblings
				last = ChangeListener.#markAndRemoveAllSiblings(this.#rootRight);
			}
			ChangeListener.#resolveMarked(start);
			return;
		}
		ChangeListener.#markAndRemoveAllSiblings(this);
		ChangeListener.#resolveMarked(this);
	}

	/**
	 * Enables use in `using` statement. Causes this listener to change when going out of scope.
	 */
	[Symbol.dispose](): void {
		this.change();
	}

	/**
	 * Helper which marks a {@link ChangeListener} and all of its siblings as changed.
	 * Also detaches them from their respective roots, and removes the sibling chain.
	 * To facilitate calling {@link ChangeListener.#resolve}, {@link ChangeListener.#root} is used
	 * to maintain the chain.  For each link in the chain, its next sibling is set as its
	 * {@link ChangeListener.#root}.
	 * The final link in the chain has itself set  as {@link ChangeListener.#root}.
	 *
	 * @param start First link in the sibling chain
	 * @returns The final link, just before looping back to `start`
	 */
	static #markAndRemoveAllSiblings(start: ChangeListener): ChangeListener {
		let sibling = start;
		while (true) {
			sibling.#changed = true;
			// Detach from root
			sibling.#rootLeft.#rootRight = sibling.#rootRight;
			sibling.#rootRight.#rootLeft = sibling.#rootLeft;
			sibling.#rootLeft = sibling.#rootRight = sibling;
			if (sibling.#sibling === start) {
				// Sibling chain ends when it loops around
				sibling.#root = sibling.#sibling = sibling;
				return sibling;
			}
			// Mark sibling and detach sibling chain
			sibling.#root = sibling.#sibling;
			sibling.#sibling = sibling;
			sibling = sibling.#root;
		}
	}

	/**
	 * Calls {@link ChangeListener.#resolve} on a chain of marked listeners starting with `start`.
	 * Follows the chain via {@link ChangeListener.#root} until reaching a listener that has itself
	 * as {@link ChangeListener.#root}.
	 * Also removes the chain by setting the {@link ChangeListener.#root} of each  link to itself.
	 *
	 * @param start First link in the chain to resolve
	 */
	static #resolveMarked(start: ChangeListener): void {
		while (true) {
			const next = start.#root;
			// First remove marking, making this listener completely isolated
			start.#root = start;
			// Call resolve
			if (start.#resolve !== null) {
				start.#resolve();
			}
			if (start === next) {
				// Chain ends when it links to itself
				break;
			}
			start = next;
		}
	}
}
