import type {
	Abort,
} from './abort.ts';
import {
	ChangeListener,
} from './change.ts';

// Waits for `millis` milliseconds
// Throws immediately if `abort` aborts while waiting
export async function waitTimeout(millis: number, options: {
	abort?: Abort,
} = {}): Promise<void> {
	const {
		abort,
	} = options;
	const changeRoot = new ChangeListener();
	let timedOut = false;
	const timeoutRef = setTimeout(() => {
		timedOut = true;
		// Signal to the listener that a timeout has occurred
		changeRoot.change();
	}, millis);
	try {
		while (true) {
			const listener = new ChangeListener(changeRoot);
			try {
				// Check for abort
				if (abort) {
					listener.addRoot(abort.changeRoot);
					if (abort.aborted) {
						throw abort.reason;
					}
				}
				// Check for timeout
				if (timedOut) {
					return;
				}

				// Wait for timeout or abort
				await listener.changed;
			}
			finally {
				listener.change();
			}
		}
	}
	finally {
		!timedOut && clearTimeout(timeoutRef);
	}
}
