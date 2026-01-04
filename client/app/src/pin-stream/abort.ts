import {
	ChangeListener,
} from './change.ts';

export class Abort {
	#aborted: boolean;
	#reason: unknown;
	#changeRoot: ChangeListener;

	constructor() {
		this.#aborted = false;
		this.#reason = undefined;
		this.#changeRoot = new ChangeListener();
	}

	// Returns `true` if `abort` has been called, otherwise `false`
	get aborted(): boolean {
		return this.#aborted;
	}

	// If `abort` has been called, returns the value of the `reason` parameter, otherwise `undefined`
	get reason(): unknown {
		return this.#reason;
	}

	// Returns a change root that changes once when `abort` is called
	get changeRoot(): ChangeListener {
		return this.#changeRoot;
	}

	// If called for the first time, sets `aborted` to `true`, `reason` to the `reason` parameter, and call `change` on `changeRoot`
	// From the second call onward, does nothing
	abort(reason?: unknown): void {
		if (this.#aborted) {
			return;
		}
		this.#aborted = true;
		this.#reason = reason;
		this.#changeRoot.change();
	}
}
