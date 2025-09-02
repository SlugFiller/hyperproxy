import {
	Readable,
	Writable
} from '@types/streamx';

export type StreamTransformer<T> = (source: (options?: { signal?: AbortSignal }) => AsyncGenerator<Uint8Array<ArrayBuffer>, void, void>, options?: { signal?: AbortSignal }) => AsyncGenerator<Uint8Array<ArrayBuffer>, T, void>;

export function combineStreams(...streams: StreamTransformer[]) : StreamTransformer<void>;

export function runStream(stream: StreamTransformer<T>, options?: { signal?: AbortSignal }): Promise<T>;

export function readerFromNodeStream(stream: Readable | Duplex): StreamTransformer<void>;

export function writerFromNodeStream(stream: Writable | Duplex): StreamTransformer<void>;

export interface Packeter extends AsyncGenerator<Uint8Array<ArrayBuffer>, void, void> {
	consume(amount: number): void;
};

export function streamPacketer(stream: AsyncIterable<Uint8Array<ArrayBuffer>>): AsyncGenerator<Packeter, void, void>;

export function anyPacket(packeter: Packeter): Promise<boolean>;

export function consumeUInt32LE(packeter: Packeter): Promise<number>;

export function consumeBuffer(packeter: Packeter): Promise<Uint8Array<ArrayBuffer>>;

export function packetUInt32LE(value: number): Promise<Uint8Array<ArrayBuffer>>;

export class StreamSplitter {
	constructor(stream?: StreamTransformer<void>);
	createStream(stream: StreamTransformer<void>): void;
	get split(): StreamTransformer<void>;
}

export class PacketSender<T> {
	constructor(options?: { signal?: AbortSignal });
	push(packet: T): void;
	shift(): Promise<T>;
}
