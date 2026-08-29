if (typeof Symbol.asyncDispose === 'undefined') {
	Object.defineProperty(Symbol, 'asyncDispose', { value: Symbol('asyncDispose') });
}
if (typeof Symbol.dispose === 'undefined') {
	Object.defineProperty(Symbol, 'dispose', { value: Symbol('dispose') });
}
export {};
