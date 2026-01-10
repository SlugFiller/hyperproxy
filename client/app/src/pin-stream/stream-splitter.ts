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

import b4a from 'b4a';
import {
	Abort,
} from './abort.ts';
import {
	ChangeListener,
} from './change.ts';
import {
	fragmentPackets,
} from './fragment.ts';
import {
	createPipe,
	receiveValue,
	sendFinish,
	sendValue,
} from './pin-stream.ts';
import type {
	PipeReadPin,
	PipeState,
	PipeWritePin,
} from './pin-stream.ts';
import {
	runProcesses,
} from './processes.ts';
import {
	decodePacketStream,
	transformPacketStream,
} from './transform.ts';
import type {
	PacketProcessResult,
} from './transform.ts';
import {
	Unrace,
} from './unrace.ts';

type StreamSplitterMessage = {
	// Sent when local stream is created
	type: 'create_stream',
	// A unique id for the stream. Subsequent messages related to this stream will contain this id
	// Note that ids from sent `create_stream` messages (local streams) may overlap ids from received `create_stream` messages (remote streams)
	// The two types of streams have distinct id namespaces
	id: number,
} | {
	// Sent to indicate no more messages will be sent for a remote stream
	// May only be sent after it is guaranteed no more messages would be received, either
	// This can happen in one of the following cases
	// 1. The stream's read pin and write pin are both in `finished` or `no_more` state, as confirmed by incoming messages
	// 2. Abort was received
	// Once this message is received, the specified stream id can and should be reused in subsequent `create_stream` messages
	type: 'release_stream',
	id: number,
} | {
	// Sent when a pin's state has changed
	// May not be sent if `abort` was already sent for this stream
	type: 'state_change',
	// The id of the stream as it was determined through the `create_stream` messages
	id: number,
	// `true` if this is a remote stream, i.e. a stream initiated due to a `create_stream` being received
	// `false` if this is a local stream, i.e. a stream for which a `create_stream` was sent to the remote
	remoteStream: boolean,
	// 'true' if the state change is the stream's read pin
	// 'false' if the state change is the stream's write pin
	readPin: boolean,
	// The new state
	newState: PipeState<Uint8Array>,
} | {
	// Sent when the stream was aborted
	// May not be sent if `abort` wa already sent for this stream,
	// or if `state_change` packets were already sent indiating both
	// the read pin and write pin are in the `finished` or `no_more` states
	// If received, and sending `abort` is allowed for the stream, a reciprocal `abort` should
	// be sent to indicate this `abort` was properly received
	type: 'abort',
	// The id of the stream as it was determined through the `create_stream` messages
	id: number,
	// `true` if this is a remote stream, i.e. a stream initiated due to a `create_stream` being received
	// `false` if this is a local stream, i.e. a stream for which a `create_stream` was sent to the remote
	remoteStream: boolean,
	// Abort message, if one could be extracted from the abort reason, or empty string otherwise
	// The message is extracted by casting the abort's reason to a string
	message: string,
};

// Represents a stream created locally and sent over the joined output
export interface StreamSplitterStream {
	readPin: PipeReadPin<Uint8Array>;
	writePin: PipeWritePin<Uint8Array>;
	// If aborted, an `abort` message should be sent to the remote
	localAbort?: Abort;
	// If present, will be aborted if the remote sends an `abort` messge
	remoteAbort?: Abort;
};

// Accumulates a stream of byte streams into a single object stream
// For each stream received over `localStreams` a corresponding `create_stream` message is sent over `joinedOutput`
// Likewise, for each `create_stream` received over `joinedInput`, a stream will be written to `remoteStreams` and associated as a remote stream with the received id
// State changes from the read and write pins of both local and remote streams will result in `state_change` messages being sent over `joinedOutput`
// `state_change` messages received from `joinedInput` result in actions on the local and remote streams pins. However, note that the direction of these messages is flipped:
// A received `state_change` with `remoteStream: true` corresponds to a stream read from `localStreams`
// A received `state_change` with `remoteStream: false` corresponds to a stream read from `remoteStreams`
// A received `state_change` with `readPin: true` results in an action on the stream's `writePin` so its `Pipe`'s corresponding `readPin` transitions to the same state
// A received `state_change` with `readPin: false` results in an action on the stream's `readPin` so its `Pipe`'s corresponding `writePin` transitions to the same state
// For easy transport, buffers larger than 65535 bytes are fragmented into smaller buffers before being sent across
// On any protocol error, this function will throw
// Messages other than `create_stream` received with a stream id for a stream that doesn't exist are a protocol error
// `create_stream` received with an id that already exists, and was not released by sending `release_stream` is a protocol error
// `state_change` messages where the state transition is impossible are a protocol error
// For example, a read pin transitioning to `has_value` without being in the `wants_value` state is an error
// It is a protocol error for any message other than `release_stream` to be received for a stream where `abort` was already sent
// It is a protocol error for any message other than `release_stream` to be received for a stream where `state_change` with a state
// of `finished` or `no_more` was already received for both the read pin and write pin
// If the provided `Abort` is aborted, all processing immediately stops, and this function throws the abort reason
async function streamSplitterMessages(joinedInput: PipeReadPin<StreamSplitterMessage>, joinedOutput: PipeWritePin<StreamSplitterMessage>, localStreams: PipeReadPin<StreamSplitterStream>, remoteStreams: PipeWritePin<StreamSplitterStream>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	interface StreamState {
		// The stream
		stream: StreamSplitterStream;
		// Whether abort was received from the remote
		abortReceived: boolean;
		// The abort message received
		abortMessage: string;
		// The last valid state received for the read pin
		lastReceivedReadPinState: PipeState<Uint8Array>;
		// The last valid state received for the write pin
		lastReceivedWritePinState: PipeState<Uint8Array>;
		// Indicates if it is valid to send state changes
		canSendReadPinState: boolean;
		canSendWritePinState: boolean;
		// Change root to signal the state has changed
		changeRoot: ChangeListener;
	}

	// Track active streams by ID
	const streamStates = new Map<boolean, Map<number, StreamState>>([
		[false, new Map()],
		[true, new Map()],
	]);

	// Generate unique IDs for local streams
	let nextLocalId = 1;
	const localIdFreeList: number[] = [];

	// Allow multiple stream runners to write to the output at the same time
	const unrace = new Unrace();

	// Helper to check if a stream is in a finished state
	function isStateFinished(state: StreamState): boolean {
		return (state.lastReceivedReadPinState.state === 'finished' || state.lastReceivedReadPinState.state === 'no_more_ack') &&
			(state.lastReceivedWritePinState.state === 'finished_ack' || state.lastReceivedWritePinState.state === 'no_more');
	}

	// Helper to run the message sending side of the stream
	async function startRunner(processes: PipeWritePin<(options?: {
		abort?: Abort,
	}) => Promise<void>>, id: number, remoteStream: boolean, stream: StreamSplitterStream, optionsStartup: {
		abort?: Abort,
	} = {}): Promise<void> {
		const commonAbort = new Abort();

		// Start message fragmenter
		const { readPin, writePin } = createPipe<Uint8Array>();
		await sendValue(processes, async () => {
			try {
				await fragmentPackets(stream.readPin, writePin, {
					// Stop when the runner stops, even if the input is still valid
					abort: commonAbort,
				});
			}
			catch (error) {
				// Do not care about errors in the fragmenter if the runner already stopped
				if (!commonAbort.aborted) {
					throw error;
				}
			}
		}, optionsStartup);

		const state: StreamState = {
			stream: {
				readPin,
				writePin: stream.writePin,
				localAbort: stream.localAbort,
				remoteAbort: stream.remoteAbort,
			},
			abortReceived: false,
			abortMessage: '',
			// Assume both pins start in `idle` state
			lastReceivedReadPinState: {
				state: 'idle',
			},
			lastReceivedWritePinState: {
				state: 'idle',
			},
			// Write pin can transition from `idle` while read pin needs to wait for a state change
			canSendReadPinState: false,
			canSendWritePinState: true,
			changeRoot: new ChangeListener(),
		};

		streamStates.get(remoteStream)!.set(id, state);

		// Start sending messages for this stream
		await sendValue(processes, async (optionsRunner = {}) => {
			const {
				abort,
			} = optionsRunner;

			try {
				if (!remoteStream) {
					// For a local stream, first send the `create_stream` message
					await unrace.run(async () => {
						await sendValue(joinedOutput, {
							type: 'create_stream',
							id,
						}, {
							abort,
							throwOnNoMore: true,
						});
					}, optionsRunner);
				}

				let sentReadPinFinish = false;
				let sentWritePinFinish = false;
				let sentIdle = false;

				while (true) {
					const listener = new ChangeListener(state.changeRoot);

					try {
						if (abort) {
							listener.addRoot(abort.changeRoot);
							if (abort.aborted) {
								throw abort.reason;
							}
						}

						// Check for stream end
						if (sentReadPinFinish && sentWritePinFinish) {
							// Must not send any state change or abort messages after this
							break;
						}

						if (state.stream.localAbort) {
							listener.addRoot(state.stream.localAbort.changeRoot);
							if (state.stream.localAbort.aborted) {
								const message = String(state.stream.localAbort.reason);
								// Send a local abort
								await unrace.run(async () => {
									await sendValue(joinedOutput, {
										type: 'abort',
										id,
										remoteStream,
										message,
									}, {
										abort,
										throwOnNoMore: true,
									});
								}, optionsRunner);
								// No more state change messages after abort
								break;
							}
						}

						if (state.abortReceived) {
							const message = state.abortMessage;
							// Send a reciprocal abort
							await unrace.run(async () => {
								await sendValue(joinedOutput, {
									type: 'abort',
									id,
									remoteStream,
									message,
								}, {
									abort,
									throwOnNoMore: true,
								});
							}, optionsRunner);
							// No more state change messages after abort
							break;
						}

						// Synchronize read pin
						if (!sentReadPinFinish && state.canSendReadPinState) {
							listener.addRoot(state.stream.readPin.changeRoot);
							const readPinState = state.stream.readPin.state;
							// Allowed state transitions for a read pin
							if ((state.lastReceivedWritePinState.state === 'wants_value' && readPinState.state === 'has_value') ||
								(state.lastReceivedWritePinState.state === 'wants_value' && readPinState.state === 'finished') ||
								(state.lastReceivedWritePinState.state === 'no_more' && readPinState.state === 'no_more_ack')) {
								// Bookkeeping to track if we already sent the final message
								if (readPinState.state === 'finished' || readPinState.state === 'no_more_ack') {
									sentReadPinFinish = true;
								}
								// Bookkeeping to prevent sending the state twice in a row
								state.canSendReadPinState = false;

								// Send the new state
								await unrace.run(async () => {
									await sendValue(joinedOutput, {
										type: 'state_change',
										id,
										remoteStream,
										readPin: true,
										newState: readPinState,
									}, {
										abort,
										throwOnNoMore: true,
									});
								}, optionsRunner);
								continue;
							}
						}

						// Synchronize write pin
						if (!sentWritePinFinish && state.canSendWritePinState) {
							listener.addRoot(state.stream.writePin.changeRoot);
							const writePinState = state.stream.writePin.state;
							// Allowed state transitions for a write pin
							if (((state.lastReceivedReadPinState.state === 'has_value' || state.lastReceivedReadPinState.state === 'idle') &&
								(writePinState.state === 'wants_value' || writePinState.state === 'no_more')) ||
								(state.lastReceivedReadPinState.state === 'has_value' && writePinState.state === 'idle') ||
								(state.lastReceivedReadPinState.state === 'finished' && writePinState.state === 'finished_ack')) {
								// Bookkeeping to track if we already sent the final message
								if (writePinState.state === 'no_more' || writePinState.state === 'finished_ack') {
									sentWritePinFinish = true;
								}

								// Have to send the switch to idle first
								if (state.lastReceivedReadPinState.state === 'has_value' && !sentIdle) {
									// Avoid sending two idles in a row
									sentIdle = true;
									await unrace.run(async () => {
										await sendValue(joinedOutput, {
											type: 'state_change',
											id,
											remoteStream,
											readPin: false,
											newState: {
												state: 'idle',
											},
										}, {
											abort,
											throwOnNoMore: true,
										});
									}, optionsRunner);
								}

								if (writePinState.state === 'idle') {
									// After idle, we still need to send no_more or wants_more before
									// the other side can send anything
									await listener.changed;
									continue;
								}

								// Bookkeeping to prevent sending the state twice in a row
								sentIdle = false;
								state.canSendWritePinState = false;

								// Send the new state
								await unrace.run(async () => {
									await sendValue(joinedOutput, {
										type: 'state_change',
										id,
										remoteStream,
										readPin: false,
										newState: writePinState,
									}, {
										abort,
										throwOnNoMore: true,
									});
								}, optionsRunner);
								continue;
							}
						}

						// Wait for any change in stream or pin states
						await listener.changed;
					}
					finally {
						listener.change();
					}
				}

				if (remoteStream) {
					// For a remote stream, finish by sending the `release_stream` message
					// But first, make sure no more messages are pending from the other side
					while (true) {
						const listener = new ChangeListener(state.changeRoot);

						try {
							if (abort) {
								listener.addRoot(abort.changeRoot);
								if (abort.aborted) {
									throw abort.reason;
								}
							}

							if (state.abortReceived || isStateFinished(state)) {
								// Done
								break;
							}

							// Wait for any change in stream state
							await listener.changed;
						}
						finally {
							listener.change();
						}
					}

					// Unregister our state. We can now receive the same id again
					streamStates.get(true)!.delete(id);

					// Send the message
					await unrace.run(async () => {
						await sendValue(joinedOutput, {
							type: 'release_stream',
							id,
						}, {
							abort,
							throwOnNoMore: true,
						});
					}, optionsRunner);
				}
			}
			catch (error) {
				// Treat unexpected error as a remote abort
				state.stream.remoteAbort && state.stream.remoteAbort.abort(error);
			}
			finally {
				commonAbort.abort();
			}
		}, optionsStartup);
	}

	await runProcesses(async (processes, optionsProc) => {
		// Start two "main" processes in parallel, each producing stream runner processes
		await runProcesses(async (mainProcesses, optionsMain) => {
			await sendValue(mainProcesses, async (optionsJoined = {}) => {
				const {
					abort,
				} = optionsJoined;

				while (true) {
					const { value: message } = await receiveValue(joinedInput, {
						abort,
						throwOnFinished: true,
					});

					switch (message.type) {
						case 'create_stream': {
							const state = streamStates.get(true)!.get(message.id);
							if (state) {
								throw new Error(`Message create_stream received for an already existing remote stream ${ message.id }`);
							}

							// Handle remote stream creation
							const { readPin: streamReadPin, writePin: streamSendWritePin } = createPipe<Uint8Array>();
							const { readPin: streamSendReadPin, writePin: streamWritePin } = createPipe<Uint8Array>();
							const localAbort = new Abort();
							const remoteAbort = new Abort();
							// Internally used stream state
							const stream: StreamSplitterStream = {
								readPin: streamReadPin,
								writePin: streamWritePin,
								localAbort,
								remoteAbort,
							}
							// Stream sent to `remoteStreams`
							const streamSend: StreamSplitterStream = {
								readPin: streamSendReadPin,
								writePin: streamSendWritePin,
								localAbort,
								remoteAbort,
							}

							if (!await sendValue(remoteStreams, streamSend, optionsJoined)) {
								// If we failed to write to `remoteStreams`, assume the stream was aborted
								localAbort.abort(new Error('Stream rejected'));
							}

							// It is necessary to pause reading from `joinedInput` until this method returns
							// This is because this method creates the stream state object that is necessary
							// for subsequent messages to be processed
							await startRunner(processes, message.id, true, stream, optionsJoined);
							break;
						}

						case 'release_stream': {
							const state = streamStates.get(false)!.get(message.id);
							if (!state) {
								throw new Error(`Message release_stream received for a non-existant local stream ${ message.id }`);
							}

							if (!state.abortReceived && !isStateFinished(state)) {
								throw new Error(`Attempt to release unfinished stream ${ message.id }`);
							}

							streamStates.get(false)!.delete(message.id);

							// We can now reuse this stream id
							localIdFreeList.push(message.id);
							break;
						}

						case 'state_change': {
							// Need to flip it since "remoteStream" is from the perspective of the sender
							const remoteStream = !message.remoteStream;

							const state = streamStates.get(remoteStream)!.get(message.id);
							if (!state) {
								throw new Error(`Message state_change received for a non-existant ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							if (state.abortReceived) {
								throw new Error(`Message state_change received for already aborted ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							if (isStateFinished(state)) {
								throw new Error(`Message state_change received for already finished ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							const newState = message.newState;

							// Apply the state change to the appropriate pin
							if (message.readPin) {
								if (state.canSendWritePinState) {
									// It is still our turn to send
									throw new Error('Read pin state change received when write pin state change send still pending')
								}
								// This is a read pin state change - need to modify the write pin
								if (newState.state === 'has_value') {
									try {
										state.stream.writePin.setValue(newState.value);
									} catch (e) {
										throw new Error(`Invalid state transition for read pin: ${ String(e) }`);
									}
								} else if (newState.state === 'finished') {
									try {
										state.stream.writePin.finish();
									} catch (e) {
										throw new Error(`Invalid state transition for read pin: ${ String(e) }`);
									}
								} else if (newState.state === 'no_more_ack') {
									try {
										state.stream.writePin.gotNoMore();
									} catch (e) {
										throw new Error(`Invalid state transition for write pin: ${ String(e) }`);
									}
								} else {
									throw new Error(`Read pin should not attempt to transition to ${ newState.state }`);
								}
								// Update the state after a successful transition
								state.lastReceivedReadPinState = newState;
								// Allowed to respond to read pin state changes with write pin state changes, unless state is final
								state.canSendWritePinState = (newState.state !== 'no_more_ack');;
							} else {
								if (state.canSendReadPinState) {
									// It is still our turn to send
									throw new Error('Write pin state change received when read pin state change send still pending')
								}
								// This is a write pin state change - need to modify the read pin
								if (newState.state === 'idle') {
									try {
										state.stream.readPin.gotValue();
									} catch (e) {
										throw new Error(`Invalid state transition for write pin: ${ String(e) }`);
									}
								} else if (newState.state === 'wants_value') {
									try {
										state.stream.readPin.wantsValue();
									} catch (e) {
										throw new Error(`Invalid state transition for write pin: ${ String(e) }`);
									}
								} else if (newState.state === 'no_more') {
									try {
										state.stream.readPin.noMore();
									} catch (e) {
										throw new Error(`Invalid state transition for write pin: ${ String(e) }`);
									}
								} else if (newState.state === 'finished_ack') {
									try {
										state.stream.readPin.gotFinish();
									} catch (e) {
										throw new Error(`Invalid state transition for write pin: ${ String(e) }`);
									}
								} else {
									throw new Error(`Write pin should not attempt to transition to ${ newState.state }`);
								}
								// Update the state after a successful transition
								state.lastReceivedWritePinState = newState;
								// Allowed to respond to write pin state changes with read pin state changes, unless state is final
								// However, cannot respond directly to idle, since wants_value or no_more must arrive first
								state.canSendReadPinState = (newState.state !== 'idle' && newState.state !== 'finished_ack');
							}

							// Inform stream runners of the state change
							state.changeRoot.change();
							break;
						}

						case 'abort': {
							// Need to flip it since "remoteStream" is from the perspective of the sender
							const remoteStream = !message.remoteStream;

							const state = streamStates.get(remoteStream)!.get(message.id);
							if (!state) {
								throw new Error(`Message abort received for a non-existant ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							if (state.abortReceived) {
								throw new Error(`Message abort received for already aborted ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							if (isStateFinished(state)) {
								throw new Error(`Message abort received for already finished ${ remoteStream ? 'remote' : 'local' } stream ${ message.id }`);
							}

							// Handle remote abort
							if (state.stream.remoteAbort) {
								state.stream.remoteAbort.abort(new Error(message.message));
							}

							// Mark abort as received
							state.abortReceived = true;
							state.abortMessage = message.message;
							state.changeRoot.change();
							break;
						}
					}
				}
			}, optionsMain);

			await sendValue(mainProcesses, async (optionsLocal) => {
				while (true) {
					const { done, value: stream } = await receiveValue(localStreams, optionsLocal);
					if (done === true) {
						break;
					}

					// Handle local stream creation

					// Get a free id
					let id = localIdFreeList.pop();
					if (id === undefined) {
						id = nextLocalId++;
					}

					await startRunner(processes, id, false, stream, optionsLocal);
				}
			}, optionsMain);

			await sendFinish(mainProcesses, optionsMain);
		}, optionsProc);

		await sendFinish(processes, optionsProc);
	}, options);
}

// Encodes a stream of `StreamSplitterMessage`s to a compact binary format
// Stops processing and throws if `abort` is aborted
async function encodeStreamSplitterMessages(input: PipeReadPin<StreamSplitterMessage>, output: PipeWritePin<Uint8Array>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	// Binary encoding: [message type (1 byte) + data]
	// Data contents:
	// Type 0: create_stream (id: 4 bytes)
	// Type 1: release_stream (id: 4 bytes)  
	// Types 2-29: state_change (id: 4 bytes, in `has_value` state: [valueLength: 2 byte, value: valueLength bytes]).
	// - Types 16-29 when remoteStream is true
	// - Types 9-15, 23-29 when readPin is true
	// - Types (2, 3, 4, 5, 6, 7, 8) + 7n for states `idle`, `wants_value`, `no_more`, `has_value`, and `finished` respectively
	// Types 30-31: abort (id: 4 bytes, remoteStream: 1 byte, messageLength: 2 byte, message: messageLength bytes).
	// - Type 31 when remoteStream is true

	await transformPacketStream(input, output, (message: StreamSplitterMessage): Promise<Uint8Array> => {
		// Encode the message
		let encoded: Uint8Array;

		switch (message.type) {
			case 'create_stream': {
				// Type 0: 1 byte + 4 bytes id
				encoded = new Uint8Array(5);
				encoded[0] = 0; // create_stream type
				new DataView(encoded.buffer).setUint32(1, message.id, true); // Little-endian
				break;
			}

			case 'release_stream': {
				// Type 1: 1 byte + 4 bytes id
				encoded = new Uint8Array(5);
				encoded[0] = 1; // release_stream type
				new DataView(encoded.buffer).setUint32(1, message.id, true); // Little-endian
				break;
			}

			case 'state_change': {
				// Types 2-29: 1 byte + 4 bytes id + (2 byte valueLength + value) (if applicable)
				const stateByte = (message.readPin ? 9 : 2) + (message.remoteStream ? 14 : 0) +
					(message.newState.state === 'idle' ? 0 :
					message.newState.state === 'wants_value' ? 1 :
					message.newState.state === 'no_more' ? 2 :
					message.newState.state === 'no_more_ack' ? 3 :
					message.newState.state === 'has_value' ? 4 :
					message.newState.state === 'finished' ? 5 : 6);

				const valueLength = message.newState.state === 'has_value' ? message.newState.value.length + 2 : 0;
				encoded = new Uint8Array(5 + valueLength);
				const dataview = new DataView(encoded.buffer);
				encoded[0] = stateByte; // state_change type
				dataview.setUint32(1, message.id, true); // Little-endian

				if (message.newState.state === 'has_value') {
					if (message.newState.value.length > 65535) {
						throw new Error('Value packet too large, please fragment');
					}
					dataview.setUint16(5, message.newState.value.length, true); // Little-endian
					encoded.set(message.newState.value, 7);
				}
				break;
			}

			case 'abort': {
				// Types 30-31: 1 byte + 4 bytes id + 2 bytes messageLength + message
				const messageBytes = b4a.from(message.message);
				if (messageBytes.length < 65536) {
					encoded = new Uint8Array(7 + messageBytes.length);
					const dataview = new DataView(encoded.buffer);
					encoded[0] = message.remoteStream ? 31 : 30; // abort type
					dataview.setUint32(1, message.id, true); // Little-endian
					dataview.setUint16(5, messageBytes.length, true); // Little-endian
					encoded.set(messageBytes, 7);
				}
				else {
					// Send truncated message
					encoded = new Uint8Array(65542);
					const dataview = new DataView(encoded.buffer);
					encoded[0] = message.remoteStream ? 31 : 30; // abort type
					dataview.setUint32(1, message.id, true); // Little-endian
					dataview.setUint16(5, 65535, true); // Little-endian
					encoded.set(messageBytes.subarray(0, 65535), 7);
				}
				break;
			}
		}

		return Promise.resolve(encoded);
	}, options);
}

// Decodes `StreamSplitterMessage`s from a binary stream encoded by `encodeStreamSplitterMessages`
// Stops processing and throws if `abort` is aborted
// Throws if the input contains bytes that cannot be decoded into valid messages
// Throws if the input ends with a partially decoded message
async function decodeStreamSplitterMessages(input: PipeReadPin<Uint8Array>, output: PipeWritePin<StreamSplitterMessage>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	await decodePacketStream(input, output, (buffer): Promise<PacketProcessResult<StreamSplitterMessage>> => {
		if (buffer.length < 1) {
			// Need more data
			return Promise.resolve({
				complete: false,
			});
		}

		const dataview = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
		const messageType = buffer[0];

		switch (messageType) {
			case 0: { // create_stream
				if (buffer.length < 5) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}
				const id = dataview.getUint32(1, true); // Little-endian
				return Promise.resolve({
					complete: true,
					packet: {
						type: 'create_stream',
						id,
					},
					bytesUsed: 5,
				});
			}

			case 1: { // release_stream
				if (buffer.length < 5) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}
				const id = dataview.getUint32(1, true); // Little-endian
				return Promise.resolve({
					complete: true,
					packet: {
						type: 'release_stream',
						id,
					},
					bytesUsed: 5,
				});
			}

			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 18:
			case 19:
			case 20:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
			case 29: { // state_change
				if (buffer.length < 5) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}
				const id = dataview.getUint32(1, true); // Little-endian
				const remoteStream = messageType >= 16;
				const readPin = messageType - (remoteStream ? 14 : 0) >= 9;
				const stateByte = messageType - (readPin ? 9 : 2) - (remoteStream ? 14 : 0);
				const state =
					stateByte === 0 ? 'idle' :
					stateByte === 1 ? 'wants_value' :
					stateByte === 2 ? 'no_more' :
					stateByte === 3 ? 'no_more_ack' :
					stateByte === 4 ? 'has_value' :
					stateByte === 5 ? 'finished' : 'finished_ack';

				if (state === 'has_value') {
					if (buffer.length < 7) {
						// Need more data for value
						return Promise.resolve({
							complete: false,
						});
					}
					const valueLength = dataview.getUint16(5, true); // Little-endian
					if (buffer.length < 7 + valueLength) {
						// Need more data for value
						return Promise.resolve({
							complete: false,
						});
					}
					const value = buffer.subarray(7, 7 + valueLength);
					return Promise.resolve({
						complete: true,
						packet: {
							type: 'state_change',
							id,
							remoteStream,
							readPin,
							newState: {
								state: 'has_value',
								value,
							},
						},
						bytesUsed: 7 + valueLength,
					});
				}

				return Promise.resolve({
					complete: true,
					packet: {
						type: 'state_change',
						id,
						remoteStream,
						readPin,
						newState: {
							state,
						},
					},
					bytesUsed: 5,
				});
			}

			case 30:
			case 31: { // abort
				if (buffer.length < 7) {
					// Need more data
					return Promise.resolve({
						complete: false,
					});
				}
				const id = dataview.getUint32(1, true); // Little-endian
				const remoteStream = messageType === 31;
				const messageLength = dataview.getUint16(5, true); // Little-endian
				if (buffer.length < 7 + messageLength) {
					// Need more data for message
					return Promise.resolve({
						complete: false,
					});
				}
				const messageText = b4a.toString(buffer.subarray(7, 7 + messageLength));
				return Promise.resolve({
					complete: true,
					packet: {
						type: 'abort',
						id,
						remoteStream,
						message: messageText,
					},
					bytesUsed: 7 + messageLength,
				});
			}

			default:
				throw new Error(`Unknown message type: ${ messageType }`);
		}
	}, options);
}

// Accumulates a stream of byte streams into a single binary stream
// For each stream received over `localStreams` a corresponding message is sent over `joinedOutput`
// Likewise, for each stream creation received over `joinedInput`, a stream will be written to `remoteStreams` and associated as a remote stream with the received id
// State changes from the read and write pins of both local and remote streams will result in synchronization messages being sent over `joinedOutput`
// Likewise, state synchronization messages received from `joinedInput` result in actions on the local and remote streams pins
// Synchronization is designed so connecting the `joinedInput` of one stream splitter to the `joinedOutput` of another, and vice versa,
// results in each local stream on one stream splitter being connected to a remote stream on the other, and vice-versa,
// with each pin on a connected stream on one stream splitter being synchronized with the matching pin of the stream from the other
// On any protocol error, this function will throw
// This includes binary errors, as well as invalid pin state transitions or messages on non-existing or finalized streams
// If the provided `Abort` is aborted, all processing immediately stops, and this function throws the abort reason
export async function streamSplitter(joinedInput: PipeReadPin<Uint8Array>, joinedOutput: PipeWritePin<Uint8Array>, localStreams: PipeReadPin<StreamSplitterStream>, remoteStreams: PipeWritePin<StreamSplitterStream>, options: {
	abort?: Abort,
} = {}): Promise<void> {
	await runProcesses(async (processes, optionsProc) => {
		const { readPin: splitterInput, writePin: decodeOutput } = createPipe<StreamSplitterMessage>();
		const { readPin: encodeInput, writePin: splitterOutput } = createPipe<StreamSplitterMessage>();
		await sendValue(processes, async (optionsSub) => {
			await streamSplitterMessages(splitterInput, splitterOutput, localStreams, remoteStreams, optionsSub);
		}, optionsProc);
		await sendValue(processes, async (optionsSub) => {
			await encodeStreamSplitterMessages(encodeInput, joinedOutput, optionsSub);
		}, optionsProc);
		await sendValue(processes, async (optionsSub) => {
			await decodeStreamSplitterMessages(joinedInput, decodeOutput, optionsSub);
		}, optionsProc);
		await sendFinish(processes, optionsProc);
	}, options);
}
