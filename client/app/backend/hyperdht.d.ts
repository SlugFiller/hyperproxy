declare module 'hyperdht' {
	import type {
		Duplex,
	} from 'bare-stream';
	import {
		EventEmitter,
	} from 'bare-events';

	interface ServerEvents {
		onconnection: [socket: Duplex];
	}

	interface HandshakePayload {
	}

	interface HandshakeResult {
		isInitiator: boolean;
		publicKey: Uint8Array;
		streamId: undefined,
		remotePublicKey: Uint8Array;
		remoteId: Uint8Array;
		holepunchSecret,
		hash: Uint8Array;
		rx: Uint8Array;
		tx: Uint8Array;
	}

	interface Handshake {
		send(payload: HandshakePayload): Uint8Array;
		recv(buf: Uint8Array): HandshakePayload | null;
		final(): HandshakeResult;
	}

	interface KeyPair {
		publicKey: Uint8Array;
		secretKey: Uint8Array;
	}

	interface Address {
		host: string;
		port: number;
	}

	type CreateHandshake = (keyPair: KeyPair, remotePublicKey: Uint8Array) => Handshake;

	type CreateSecretStream = (isInitiator: boolean, rawStream: null | Duplex, opts: {
		handshake: Handshake
		publicKey: Uint8Array,
		remotePublicKey: Uint8Array,
		autoStart: boolean,
		keepAlive: number,
	}) => Duplex

	export const FIREWALL: {
		UNKNOWN: 0;
		OPEN: 1;
		CONSISTENT: 2;
		RANDOM: 3;
	}

	type Firewall = 0 | 1 | 2 | 3;

	interface CreateServerOptions {
		onconnection?: (socket: Duplex) => void;
		firewall?: (remotePublicKey: Uint8Array, remotePayload: HandshakePayload, clientAddress: Address) => boolean | Promise<boolean>;
		holepunch?: false | ((remoteFirewall: Firewall, natFirewall: Firewall, remoteAddresses: Address[], natAddresses: Address[]) => boolean);
		relayThrough?: null | Uint8Array | (null | Uint8Array)[] | (() => (null | Uint8Array | (null | Uint8Array)[]));
		relayKeepAlive?: number;
		pool?: null | {
			_attachServer: (Server) => void,
		};
		createHandshake?: CreateHandshake;
		createSecretStream?: CreateSecretStream;
		handshakeClearWait?: number;
		shareLocalAddress?: boolean;
		reusableSocket?: boolean;
	}

	interface AnnouncerOptions {
	}

	interface ConnectOptions {
		pool?: null | Map<string, Duplex>;
		keyPair?: KeyPair;
		relayThrough?: null | Uint8Array | (null | Uint8Array)[] | (() => (null | Uint8Array | (null | Uint8Array)[]));
		createSecretStream?: CreateSecretStream;
		relayAddresses?: Address[];
		reusableSocket?: boolean;
		createHandshake?: CreateHandshake;
		localConnection?: boolean;
		relayKeepAlive?: number;
	}

	interface DestroyOptions {
		force?: boolean;
	}

	class Server extends EventEmitter<ServerEvents> {
		listen(keyPair: KeyPair, opts?: AnnouncerOptions);
	}

	class HyperDHT {
		constructor(opts?: {
			port?: number,
			bootstrap?: string[],
			nodes?: string[],
		});

		connect(remotePublicKey: Uint8Array, opts?: ConnectOptions): Duplex;

		destroy(opts?: DestroyOptions): Promise<void>;

		createServer(opts: CreateServerOptions): Server;
		createServer(opts: CreateServerOptions, onconnection?: (socket: Duplex) => void): Server;

		static keyPair(seed?: Uint8Array): KeyPair;
	}

	export = HyperDHT;
}
