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
