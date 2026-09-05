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

import { polyfillTextEncoder } from './EncoderDecoderTogether.ts';
if (typeof Symbol.asyncDispose === 'undefined') {
	Object.defineProperty(Symbol, 'asyncDispose', { value: Symbol('asyncDispose') });
}
if (typeof Symbol.dispose === 'undefined') {
	Object.defineProperty(Symbol, 'dispose', { value: Symbol('dispose') });
}
polyfillTextEncoder(globalThis);

if (!Uint8Array.prototype.toBase64) {
	Uint8Array.prototype.toBase64 = function () {	// eslint-disable-line no-extend-native
		return btoa(String.fromCharCode(...this));
	};
}

declare global {
	interface TextDecodeOptions {
		stream?: boolean;
	}

	interface TextDecoderOptions {
		fatal?: boolean;
		ignoreBOM?: boolean;
	}

	interface TextEncoderEncodeIntoResult {
		read: number;
		written: number;
	}

	interface TextDecoder {
		readonly encoding: string;
		readonly fatal: boolean;
		readonly ignoreBOM: boolean;
		decode(input?: BufferSource, options?: TextDecodeOptions): string;
	}

	var TextDecoder: {
		prototype: TextDecoder;
		new (label?: string, options?: TextDecoderOptions): TextDecoder;
	};

	interface TextEncoder {
		readonly encoding: string;
		encode(input?: string): Uint8Array;
		encodeInto(source: string, destination: Uint8Array): TextEncoderEncodeIntoResult;
	}

	var TextEncoder: {
		prototype: TextEncoder;
		new (): TextEncoder;
	};

	interface Uint8Array {
		toBase64(): string;
	}

	function atob(encodedString: string): string;
	function btoa(bufferString: string): string;
}

export {};
