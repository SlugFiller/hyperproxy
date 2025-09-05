const weakEvent = new FinalizationRegistry(({ object, event, listener }) => {
	if (object.removeEventListener) {
		object.removeEventListener(event, listener);
	}
	else if (object.off) {
		object.off(event, listener);
	}
});

export function addWeakListener(object, event, listener, options) {
	const weak = new WeakRef(listener);
	const wrapper = (...args) => {
		weak.deref()?.(...args);
	};
	weakEvent.register(listener, { object, event, listener: wrapper });
	if (object.addEventListener) {
		object.addEventListener(event, wrapper, options);
	}
	else if (object.on) {
		object.on(event, wrapper, options);
	}
}
