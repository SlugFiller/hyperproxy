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
	useEffect,
	useState,
} from 'react';
import {
	Worklet,
} from 'react-native-bare-kit';
import {
	Duplex,
} from 'streamx';
import backendBundle from '../backend.bundle.mjs'
// Warning: You can NOT use destructuring import with a native module
import ForegroundWorklet from '../specs/NativeForegroundWorklet';

let worklet: Worklet | null = null;

export async function runWorklet() {
	if (worklet !== null) {
		return;
	}

	worklet = new Worklet();

	// Prevent app state updates from influencing the worklet, since it's running in the background anyway
	worklet.update = () => {};

	const stream = new Duplex({
		writev(buffers: Uint8Array[], callback: (err: Error | null) => void) {
			(async () => {
				let ofs = 0;
				for (const buf of buffers) {
					ofs += buf.length;
				}
				const buffer = new Uint8Array(ofs);
				ofs = 0;
				for (const buf of buffers) {
					buffer.set(buf, ofs);
					ofs += buf.length;
				}
				if (buffer.byteLength <= 0) {
					callback(null);
					return;
				}
				const encoded = buffer.toBase64();
				await ForegroundWorklet.writeWorklet(encoded)
				callback(null);
			})().catch(callback);
		},
		final(callback: (err: Error | null) => void) {
			(async () => {
				await ForegroundWorklet.writeWorklet('')
				callback(null);
			})().catch(callback);
		},
		read(callback: (err: Error | null) => void) {
			(async () => {
				const buffer = await ForegroundWorklet.readWorklet()
				if (buffer === '') {
					this.push(null);
				}
				else {
					this.push(Uint8Array.from(atob(buffer), c => c.charCodeAt(0)));
				}
				callback(null);
			})().catch(callback);
		},
	});

	stream.pipe(worklet.IPC);
	worklet.IPC.pipe(stream);

	worklet.start('/backend.bundle', backendBundle);

	// Run indefinitely (Or rather, until killed)
	await new Promise(() => {});
}

export function useForegroundWorklet() {
	const [IPC, setIPC] = useState<null | Duplex>(null);

	useEffect(() => {
		const stream = new Duplex({
			writev(buffers: Uint8Array[], callback: (err: Error | null) => void) {
				(async () => {
					let ofs = 0;
					for (const buf of buffers) {
						ofs += buf.length;
					}
					const buffer = new Uint8Array(ofs);
					ofs = 0;
					for (const buf of buffers) {
						buffer.set(buf, ofs);
						ofs += buf.length;
					}
					if (buffer.byteLength <= 0) {
						callback(null);
						return;
					}
					const encoded = buffer.toBase64();
					await ForegroundWorklet.writeMain(encoded)
					callback(null);
				})().catch(callback);
			},
			final(callback: (err: Error | null) => void) {
				(async () => {
					await ForegroundWorklet.writeMain('')
					callback(null);
				})().catch(callback);
			},
			read(callback: (err: Error | null) => void) {
				(async () => {
					const buffer = await ForegroundWorklet.readMain()
					if (buffer === '') {
						this.push(null);
					}
					else {
						this.push(Uint8Array.from(atob(buffer), c => c.charCodeAt(0)));
					}
					callback(null);
				})().catch(callback);
			},
		});

		setIPC(stream);

		ForegroundWorklet.startService();
	}, []);

	// FIXME: Pre-stream-split the IPC on the native side, to better deal with app going away mid-message
	return IPC;
}
