// FinalizationRegistry

if (typeof global.FinalizationRegistry === 'undefined') {
	const REGISTRY_SWEEP_INTERVAL = 10000;

	class PolyFinalizationRegistry {
		#counter;
		#registrations;
		#sweepTimeout;
		#finalize;
		#sweepCallback;

		constructor(finalize) {
			this.#counter = 0;
			this.#registrations = new Map();
			this.#sweepTimeout = undefined;
			this.#finalize = finalize;
			this.#sweepCallback = () => this.#sweep();
		}

		register(target, value, token) {
			this.#registrations.set(this.#counter++, {
				targetRef: new WeakRef(target),
				tokenRef: token != null ? new WeakRef(token) : undefined,
				value,
			});
			this.#scheduleSweep();
		}

		unregister(token) {
			if (token == null) {
				return;
			}
			for (const [key, registration] of this.#registrations) {
				if (registration.tokenRef?.deref() === token) {
					this.#registrations.delete(key);
				}
			}
			if (this.#sweepTimeout && this.#registrations.size <= 0) {
				clearTimeout(this.#sweepTimeout);
				this.#sweepTimeout = undefined;
			}
		}

		#sweep() {
			clearTimeout(this.#sweepTimeout);
			this.#sweepTimeout = undefined;
			for (const [key, registration] of this.#registrations) {
				if (registration.targetRef.deref() !== undefined) {
					return;
				}
				this.#registrations.delete(key);
				try {
					this.#finalize(registration.value);
				}
				catch {
				}
			}
			if (this.#registrations.size > 0) {
				this.#scheduleSweep();
			}
		}

		#scheduleSweep() {
			if (this.#sweepTimeout) {
				return;
			}
			this.#sweepTimeout = setTimeout(this.#sweepCallback, REGISTRY_SWEEP_INTERVAL)
		}
	}

	global.FinalizationRegistry = PolyFinalizationRegistry;
}

if (typeof global.AbortSignal === 'undefined' || typeof global.AbortSignal.abort === 'undefined' || typeof global.AbortSignal.any === 'undefined') {
	const regSignals = new FinalizationRegistry(({ weakparam, remover }) => {
		const param = weakparam.deref();
		if (param) {
			remover(param);
		}
	});

	const strongRefs = new Set();

	class PolyAbortSignal {
		#listener;
		#aborted;
		#reason;
		#listeners;
		#refcount;
		#refobject;

		constructor(subscribe) {
			this.#listener = (reason) => {
				this.#doAbort(reason);
			};
			this.#aborted = false;
			this.#listeners = new Map();
			this.#refcount = 0;
			this.#refobject = null;
			subscribe && subscribe(this.#listener);
		}

		addEventListener(type, listener, { signal } = {}) {
			if (type !== 'abort' || this.#aborted) {
				return;
			}
			let object = this.#listeners.get(listener);
			if (!object) {
				object = {
					refcounting: !!this.#refobject,
					remove: () => {
						this.removeEventListener('abort', listener);
					},
					signal: null,
				};
				const weak = new WeakRef(object);
				object.weakremove = () => {
					const obj = weak.deref();
					if (!obj) {
						return;
					}
					obj.remove();
				};
				this.#listeners.set(listener, object);
				if (this.#refobject) {
					this.#refcount++;
					if (this.#refcount === 1) {
						strongRefs.add(this.#refobject);
					}
				}
			}
			const prevSignal = object.signal?.deref();
			if (prevSignal === signal) {
				return;
			}
			if (prevSignal) {
				prevSignal.removeEventListener('abort', object.weakremove);
			}
			if (object.signal) {
				regSignals.unregister(object);
			}
			if (signal) {
				object.signal = new WeakRef(signal);
				signal.addEventListener('abort', object.weakremove);
				regSignals.register(object, { weakparam: object.signal, remover: (sig) => {
					sig.removeEventListener('abort', object.weakremove);
				} }, object);
			}
			else {
				object.signal = null;
			}
		}

		removeEventListener(type, listener) {
			if (type !== 'abort' || this.#aborted) {
				return;
			}
			let object = this.#listeners.get(listener);
			if (!object) {
				return;
			}
			this.#listeners.delete(listener);
			const prevSignal = object.signal?.deref();
			if (prevSignal) {
				prevSignal.removeEventListener('abort', object.weakremove);
			}
			if (object.signal) {
				regSignals.unregister(object);
			}
			if (object.refcounting) {
				this.#refcount--;
				if (this.#refcount <= 0) {
					strongRefs.delete(this.#refobject);
				}
			}
		}

		get aborted() {
			return this.#aborted;
		}

		get reason() {
			return this.#reason;
		}

		throwIfAborted() {
			if (this.#aborted) {
				throw this.#reason;
			}
		}

		static abort(reason) {
			const controller = new PolyAbortController();
			controller.abort(reason);
			return controller.signal;
		}

		static any(signals) {
			const controller = new PolyAbortController();
			const weakController = new WeakRef(controller);
			const weakSignals = new Set();
			const removesignal = (object) => {
				weakSignals.delete(object);
				regSignals.unregister(object.ref1);
				regSignals.unregister(object.ref2);
				object.signal.deref()?.removeEventListener('abort', object.weakabort);
			};
			const weakabort = (reason) => {
				weakController.deref()?.abort();
				for (const object of weakSignals) {
					regSignals.unregister(object.ref1);
					regSignals.unregister(object.ref2);
					object.signal.deref()?.removeEventListener('abort', object.weakabort);
				}
				weakSignals.clear();
			};
			controller.signal.addEventListener('abort', weakabort);
			controller.signal.#refobject = controller;
			for (const signal of signals) {
				if (!signal) {
					continue;
				}
				if (signal.aborted) {
					controller.abort(signal.reason);
					return;
				}
				const object = {
					ref1: {},
					ref2: {},
					signal: new WeakRef(signal),
				};
				const weak = new WeakRef(object);
				object.weakabort = () => {
					const obj = weak.deref();
					if (!obj) {
						return;
					}
					const sig = obj.signal.deref();
					if (!sig) {
						return;
					}
					weakabort(sig.reason);
				};
				signal.addEventListener('abort', object.weakabort);
				regSignals.register(controller, { weakparam: new WeakRef(object), remover: removesignal }, object.ref1);
				regSignals.register(signal, { weakparam: new WeakRef(object), remover: removesignal }, object.ref2);
				weakSignals.add(object);
			}
			return controller.signal;
		}

		#doAbort(reason) {
			if (this.#aborted) {
				return;
			}
			if (this.#refobject) {
				strongRefs.delete(this.#refobject);
			}
			this.#aborted = true;
			this.#reason = reason;
			for (const [listener, object] of this.#listeners) {
				queueMicrotask(listener);
				const prevSignal = object.signal?.deref();
				if (prevSignal) {
					prevSignal.removeEventListener('abort', object.weakremove);
				}
				if (object.signal) {
					regSignals.unregister(object);
				}
			}
			this.#listeners.clear();
		}
	}

	class PolyAbortController {
		#onabort;
		#signal;

		constructor() {
			this.#signal = new PolyAbortSignal((onabort) => {
				this.#onabort = onabort;
			});
		}

		abort(reason) {
			this.#onabort(reason || new Error('Aborted'));
		}

		get signal() {
			return this.#signal;
		}
	}

	global.AbortSignal = PolyAbortSignal;
	global.AbortController = PolyAbortController;
}

if (!Symbol.asyncIterator) {
	Symbol.asyncIterator = Symbol('asyncIterator');
	Object.getPrototypeOf(Object.getPrototypeOf(async function*(){}()))[Symbol.asyncIterator] = function() {
		return this;
	};
}
