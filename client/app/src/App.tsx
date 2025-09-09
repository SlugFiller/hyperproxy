/**
 * Sample React Native App
 * https://github.com/facebook/react-native
 *
 * @format
 */

import './polyfills.mjs'
import {
	entropyToMnemonic,
	mnemonicToEntropy,
} from '@scure/bip39';
import {
	wordlist,
} from '@scure/bip39/wordlists/english';
import b4a from 'b4a';
import {
	Children,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from 'react';
import type {
	FC,
	ReactNode,
	RefAttributes,
} from 'react';
import {
	ActivityIndicator,
	FlatList,
	Linking,
	Pressable,
	StatusBar,
	StyleSheet,
	Text,
	TextInput,
	View,
	useColorScheme,
} from 'react-native';
import type {
	StyleProp,
	TextStyle,
	ViewProps,
	ViewStyle,
} from 'react-native';
import {
	enableSimpleNullHandling,
	open,
} from 'react-native-nitro-sqlite';
import {
	KeyboardAvoidingView,
	KeyboardProvider,
} from 'react-native-keyboard-controller';
import {
	SafeAreaProvider,
	SafeAreaView,
} from 'react-native-safe-area-context';
import ToastManager, {
	Toast,
} from 'toastify-react-native'
import {
	useForegroundWorklet,
} from './foreground-worklet.ts'
import {
	StreamSplitter,
	anyPacket,
	combineStreams,
	consumeBuffer,
	consumeUInt32LE,
	packetUInt32LE,
	readerFromNodeStream,
	runStream,
	streamPacketer,
	writerFromNodeStream,
} from './parse-utils.mjs'
import {
	RPC_KEYGEN,
	RPC_LIST,
	RPC_PROXY,
	RPC_UNPROXY,
	RPC_IS_PROXY,
	RPC_UNPROXY_ALL,
} from '../backend/rpc-commands.mjs';

interface AbortSignalStatic {
	any(signals: Iterable<AbortSignal>): AbortSignal;
}

enableSimpleNullHandling();

const db = open({ name: 'config.sqlite' });
db.execute(`CREATE TABLE IF NOT EXISTS keypair(
	publicKey BLOB,
	secretKey BLOB
)`);
db.execute(`CREATE TABLE IF NOT EXISTS server(
	name TEXT,
	publicKey BLOB
)`);
db.execute(`CREATE UNIQUE INDEX IF NOT EXISTS server__name ON server (name)`);

function toBuffer(typedarray: Uint8Array<ArrayBuffer>): ArrayBuffer {
	return typedarray.buffer.slice(typedarray.byteOffset, typedarray.byteOffset + typedarray.byteLength);
}

const App: FC = () => {
	const isDarkMode = useColorScheme() === 'dark';

	return (
		<KeyboardProvider>
			<SafeAreaProvider>
				<StatusBar barStyle={isDarkMode ? 'light-content' : 'dark-content'} />
				<AppContent />
			</SafeAreaProvider>
		</KeyboardProvider>
	);
};

interface KeyPair {
	publicKey: Uint8Array<ArrayBuffer>;
	secretKey: Uint8Array<ArrayBuffer>;
}

type KeyPairStatus =
	| {
		status: 'loading',
	}
	| {
		status: 'valid',
		publicKey: Uint8Array<ArrayBuffer>,
		secretKey: Uint8Array<ArrayBuffer>,
	}
	| {
		status: 'error',
		error: Error,
	}
;

const AppContent: FC = () => {
	type SelectedView =
		| {
			view: 'list',
		}
		| {
			view: 'key',
		}
		| {
			view: 'show',
			name: string,
			key: Uint8Array<ArrayBuffer>,
		}
		| {
			view: 'service',
			name: string,
			key: Uint8Array<ArrayBuffer>,
			service: string,
		}
		| {
			view: 'add',
		}
		| {
			view: 'edit',
			name: string,
		}
	;

	const [serverListReload, setServerListReload] = useState<Record<string, never>>({});
	const [serverName, setServerName] = useState<string>('');
	const [phrase, setPhrase] = useState<string>(' '.repeat(23));
	const [keyPair, setKeyPair] = useState<KeyPairStatus>({ status: 'loading' });
	const [selectedView, setSelectedView] = useState<SelectedView>({ view: 'list' });

	let phraseDecodable: boolean = false;
	try {
		mnemonicToEntropy(phrase, wordlist);
		phraseDecodable = true;
	}
	catch {
	}

	const IPC = useForegroundWorklet();

	const splitter = useMemo(() => {
		return new StreamSplitter();
	}, []);

	useEffect(() => {
		if (!IPC) {
			return;
		}

		const controller = new AbortController();
		runStream(combineStreams(
			readerFromNodeStream(IPC),
			splitter.split,
			writerFromNodeStream(IPC)
		), { signal: controller.signal }).catch(() => {
			// If we're here, it's most likely due to abort. Ignore
		});

		return () => {
			controller.abort();
		};
	}, [IPC, splitter]);

	useEffect(() => {
		const controller = new AbortController();

		(async () => {
			// Attempt to load key from storage
			const keyPairRow = (await db.executeAsync<{ publicKey: ArrayBuffer, secretKey: ArrayBuffer }>(`SELECT publicKey, secretKey FROM keypair LIMIT 1`)).rows?.item(0);
			if (keyPairRow) {
				if (controller.signal.aborted) {
					return;
				}
				setKeyPair({
					status: 'valid',
					publicKey: new Uint8Array(keyPairRow.publicKey),
					secretKey: new Uint8Array(keyPairRow.secretKey),
				});
				return;
			}

			// Failed. Generate new key
			const newKeyPair = await new Promise<KeyPair>((resolve) => {
				splitter.createStream(async function* (stream, { signal } = {}) {
					yield packetUInt32LE(RPC_KEYGEN);
					for await (const packeter of streamPacketer(stream({ signal: signal ? (AbortSignal as unknown as AbortSignalStatic).any([signal, controller.signal]) : controller.signal }))) {
						const publicKey = await consumeBuffer(packeter);
						const secretKey = await consumeBuffer(packeter);
						resolve({
							publicKey,
							secretKey,
						});
					}
				});
			});
			if (controller.signal.aborted) {
				return;
			}
			setKeyPair({
				status: 'valid',
				publicKey: newKeyPair.publicKey,
				secretKey: newKeyPair.secretKey,
			});
			db.execute(`INSERT INTO keypair(publicKey, secretKey) SELECT ?, ?
				FROM (SELECT 1 t) t LEFT JOIN keypair e ON (1=1) WHERE e.ROWID IS NULL`,
				[
					toBuffer(newKeyPair.publicKey),
					toBuffer(newKeyPair.secretKey),
				]
			);
		})().catch((error: Error) => {
			if (controller.signal.aborted) {
				return;
			}
			setKeyPair({
				status: 'error',
				error,
			});
		});

		return () => {
			controller.abort();
		};
	}, [splitter]);

	const selectServer = useCallback((name: string, key: Uint8Array<ArrayBuffer>) => {
		setSelectedView({
			view: 'show',
			name,
			key,
		});
	}, [setSelectedView]);

	const selectService = useCallback((service: string) => {
		setSelectedView((selected) => selected.view !== 'show' ? selected : {
			view: 'service',
			name: selected.name,
			key: selected.key,
			service,
		});
	}, [setSelectedView]);

	const addServer = useCallback(() => {
		try {
			if (serverName === '') {
				throw new Error('Server name empty');
			}
			const serverKey = mnemonicToEntropy(phrase, wordlist) as Uint8Array<ArrayBuffer>;
			// Queries that use buffer parameters must be sync
			db.execute(`REPLACE INTO server(name, publicKey) VALUES (?, ?)`, [serverName, toBuffer(serverKey)]);
			setServerListReload({});
			setSelectedView({
				view: 'list',
			});
		}
		catch (error) {
			error instanceof Error && Toast.error(error.message);
		}
	}, [serverName, phrase, setServerListReload]);

	const editServer = useCallback((name: string) => {
		try {
			if (serverName === '') {
				throw new Error('Server name empty');
			}
			const serverKey = mnemonicToEntropy(phrase, wordlist) as Uint8Array<ArrayBuffer>;
			db.execute(`UPDATE OR REPLACE server SET name = ?, publicKey = ? WHERE name = ?`, [serverName, toBuffer(serverKey), name]);
			setServerListReload({});
			setSelectedView({
				view: 'list',
			});
		}
		catch (error) {
			error instanceof Error && Toast.error(error.message);
		}
	}, [serverName, phrase, setServerListReload, setSelectedView]);

	const deleteServer = useCallback((name: string) => {
		try {
			if (serverName === '') {
				throw new Error('Server name empty');
			}
			const { rows } = db.execute<{ publicKey: ArrayBuffer }>(`SELECT publicKey FROM server WHERE name = ?`, [name]);
			if (!rows || rows.length < 1) {
				throw new Error('Server not found');
			}
			const serverKey = new Uint8Array(rows.item(0)!.publicKey);
			db.execute(`DELETE FROM server WHERE name = ?`, [name]);
			splitter.createStream(async function* (stream, { signal } = {}) {
				yield packetUInt32LE(RPC_UNPROXY_ALL);
				yield packetUInt32LE(serverKey.byteLength);
				yield serverKey;
				for await (const _discard of stream({ signal }));
				setServerListReload({});
				setSelectedView({
					view: 'list',
				});
			});
		}
		catch (error) {
			error instanceof Error && Toast.error(error.message);
		}
	}, [serverName, setServerListReload, setSelectedView, splitter]);

	const moveToEdit = useCallback((name: string, key: Uint8Array<ArrayBuffer>) => {
		setServerName(name);
		setPhrase(entropyToMnemonic(key, wordlist));
		setSelectedView({
			view: 'edit',
			name: name,
		});
	}, [setServerName, setPhrase, setSelectedView]);

	return (
		<SafeAreaView style={styles.container}>
			<KeyboardAvoidingView behavior={"height"} style={styles.container}>
				<View style={styles.content}>
					<View style={selectedView.view === 'list' ? styles.container : styles.hidden}>
						<ServerList onSelect={selectServer} reload={serverListReload} />
					</View>
					{(() => { switch (selectedView.view) {
						case 'key': return (
							<KeyView keyPair={keyPair} />
						);
						case 'show': return (<>
							<View style={styles.editNameContainer}>
								<Text style={styles.serverNameLabel}>Server name:</Text>
								<Text style={styles.serverNameText}>{selectedView.name}</Text>
							</View>
							<ServiceList
								keyPair={keyPair}
								serverKey={selectedView.key}
								streamSplitter={splitter}
								onSelect={selectService}
							/>
						</>)
						case 'service': return (<>
							<View style={styles.editNameContainer}>
								<Text style={styles.serverNameLabel}>Server name:</Text>
								<Text style={styles.serverNameText}>{selectedView.name}</Text>
							</View>
							<View style={styles.editNameContainer}>
								<Text style={styles.serverNameLabel}>Service name:</Text>
								<Text style={styles.serverNameText}>{selectedView.service}</Text>
							</View>
							<ServiceProxy
								keyPair={keyPair}
								serverKey={selectedView.key}
								serviceName={selectedView.service}
								streamSplitter={splitter}
							/>
						</>)
						case 'add':
						case 'edit': return (<>
							<View style={styles.editNameContainer}>
								<Text style={styles.serverNameLabel}>Server name:</Text>
								<TextInput value={serverName} onChangeText={setServerName} style={[styles.input]} />
							</View>
							<Bip39Phrase
								value={phrase}
								setValue={setPhrase}
								numColumns={4}
								style={styles.bipPhrase}
								cellStyle={styles.bipCell}
							/>
						</>)
					} })()}
				</View>
				<View style={styles.buttonDrawer}>
					{selectedView.view === 'list' ? (<>
						<TextButton
							title="Add"
							onPress={() => setSelectedView({ view: 'add' })}
							style={styles.button}
							stylePressed={styles.buttonPressed}
							styleText={styles.buttonText}
							stylePressedText={styles.buttonPressedText}
						/>
						<TextButton
							title="Show my key"
							onPress={() => setSelectedView({ view: 'key' })}
							style={styles.button}
							stylePressed={styles.buttonPressed}
							styleText={styles.buttonText}
							stylePressedText={styles.buttonPressedText}
						/>
					</>) : (<>
						<TextButton
							title="Back"
							onPress={() => setSelectedView((selected) => selected.view !== 'service' ? { view: 'list' } : { view: 'show', name: selected.name, key: selected.key })}
							style={styles.button}
							stylePressed={styles.buttonPressed}
							styleText={styles.buttonText}
							stylePressedText={styles.buttonPressedText}
						/>
						{(() => { switch (selectedView.view) {
							case 'show': return (
								<TextButton
									title="Edit"
									onPress={() => moveToEdit(selectedView.name, selectedView.key)}
									style={styles.button}
									stylePressed={styles.buttonPressed}
									styleText={styles.buttonText}
									stylePressedText={styles.buttonPressedText}
								/>
							)
							case 'add': return (
								<TextButton
									title="Add"
									disabled={serverName === '' || !phraseDecodable}
									onPress={addServer}
									style={styles.button}
									stylePressed={styles.buttonPressed}
									styleDisabled={styles.buttonDisabled}
									styleText={styles.buttonText}
									stylePressedText={styles.buttonPressedText}
									styleDisabledText={styles.buttonDisabledText}
								/>
							)
							case 'edit': return (<>
								<TextButton
									title="Delete"
									onPress={() => deleteServer(selectedView.name)}
									style={styles.button}
									stylePressed={styles.buttonPressed}
									styleText={styles.buttonText}
									stylePressedText={styles.buttonPressedText}
								/>
								<TextButton
									title="Save"
									disabled={serverName === '' || !phraseDecodable}
									onPress={() => editServer(selectedView.name)}
									style={styles.button}
									stylePressed={styles.buttonPressed}
									styleDisabled={styles.buttonDisabled}
									styleText={styles.buttonText}
									stylePressedText={styles.buttonPressedText}
									styleDisabledText={styles.buttonDisabledText}
								/>
							</>)
						} })()}
					</>)}
				</View>
			</KeyboardAvoidingView>
			<ToastManager
				position="center"
			/>
		</SafeAreaView>
	);
};

interface KeyViewProps {
	keyPair: KeyPairStatus;
}

const KeyView: FC<KeyViewProps> = ({ keyPair }) => {
	switch (keyPair.status) {
		case 'loading': return (
			<ActivityIndicator size="large" />
		);
		case 'error': return (
			<View>
				<Text>Error</Text>
				<Text>{keyPair.error.message}</Text>
				<Text>{keyPair.error.stack}</Text>
			</View>
		);
		case 'valid': return (
			<Bip39Display
				value={entropyToMnemonic(keyPair.publicKey, wordlist)}
				numColumns={4}
				style={styles.bipPhrase}
				cellStyle={styles.bipCell}
				textStyle={styles.bipText}
			/>
		);
	}
};

interface ServerListProps {
	onSelect?: (name: string, key: Uint8Array<ArrayBuffer>) => void;
	reload: Record<string, never>;
}

const ServerList: FC<ServerListProps> = ({ onSelect, reload }) => {
	interface Server {
		name: string;
		key: Uint8Array<ArrayBuffer>;
	}

	type ServerListStatus =
		| {
			status: 'loading',
		}
		| {
			status: 'error',
			error: Error,
		}
		| {
			status: 'valid',
			list: Server[],
		}
	;

	const [serverList, setServerList] = useState<ServerListStatus>({ status: 'loading' });

	useEffect(() => {
		let active = true;
		(async () => {
			const { rows } = await db.executeAsync<{ name: string, publicKey: ArrayBuffer }>(`SELECT name, publicKey FROM server ORDER BY name ASC`);
			if (!active) {
				return;
			}
			if (!rows) {
				setServerList({ status: 'valid', list: [] });
				return;
			}
			const list: Server[] = [];
			for (let i = 0; i < rows.length; i++) {
				const row = rows.item(i)!;
				list.push({
					name: row.name,
					key: new Uint8Array(row.publicKey),
				});
			}
			setServerList({
				status: 'valid',
				list,
			});
		})().catch((error: Error) => {
			if (!active) {
				return;
			}
			setServerList({
				status: 'error',
				error,
			});
		});
		return () => {
			active = false;
		};
	}, [setServerList, reload]);

	switch (serverList.status) {
		case 'loading': return (
			<ActivityIndicator size="large" />
		);
		case 'error': return (
			<View>
				<Text>Error</Text>
				<Text>{serverList.error.message}</Text>
				<Text>{serverList.error.stack}</Text>
			</View>
		);
		case 'valid': return (
			<FlatList
				data={serverList.list}
				keyExtractor={({ name }: Server) => name}
				renderItem={({ item: { name, key } }: { item: Server }) => onSelect ? (
					<Pressable onPress={() => onSelect(name, key)}>
						{({ pressed }) => (
							<Text style={pressed ? [styles.serverListItem, styles.serverListItemPressed] : [styles.serverListItem]}>{name}</Text>
						)}
					</Pressable>
				): (
					<Text style={[styles.serverListItem]}>{name}</Text>
				)}
				ItemSeparatorComponent={ServerListSeparatorComponent}
			/>
		);
	}
};

const ServerListSeparatorComponent: FC = () => {
	return (
		<View style={styles.serverListSeparator} collapsable={false} />
	);
};

interface ServiceListProps {
	onSelect?: (name: string) => void;
	keyPair: KeyPairStatus;
	serverKey: Uint8Array<ArrayBuffer>;
	streamSplitter: StreamSplitter;
}

const ServiceList: FC<ServiceListProps> = ({ keyPair, serverKey, streamSplitter: splitter, onSelect }) => {
	const [services, setServices] = useState<string[]>([]);
	const [loading, setLoading] = useState<boolean>(true);

	useEffect(() => {
		if (keyPair.status !== 'valid') {
			return;
		}

		setLoading(true);

		const controller = new AbortController();
		const gotServices: string[] = [];

		splitter.createStream(async function* (stream, { signal } = {}) {
			yield packetUInt32LE(RPC_LIST);
			yield packetUInt32LE(keyPair.publicKey.byteLength);
			yield keyPair.publicKey;
			yield packetUInt32LE(keyPair.secretKey.byteLength);
			yield keyPair.secretKey;
			yield packetUInt32LE(serverKey.byteLength);
			yield serverKey;
			for await (const packeter of streamPacketer(stream({ signal: signal ? (AbortSignal as unknown as AbortSignalStatic).any([signal, controller.signal]) : controller.signal }))) {
				while (await anyPacket(packeter)) {
					const service = b4a.toString(await consumeBuffer(packeter));
					gotServices.push(service);
					setServices([...gotServices]);
				}
			}
			setLoading(false);
		});

		return () => {
			controller.abort();
		};
	}, [keyPair, serverKey, splitter]);

	return (
		<View style={styles.container}>
			{loading && (
				<ActivityIndicator size="large" style={styles.floatingLoader} />
			)}
			{services.length && (
				<FlatList
					data={services}
					keyExtractor={(name) => name}
					renderItem={({ item: name }: { item: string }) => onSelect ? (
						<Pressable onPress={() => onSelect(name)}>
							{({ pressed }) => (
								<Text style={pressed ? [styles.serviceListItem, styles.serviceListItemPressed] : [styles.serviceListItem]}>{name}</Text>
							)}
						</Pressable>
					): (
						<Text style={[styles.serviceListItem]}>{name}</Text>
					)}
					ItemSeparatorComponent={ServiceListSeparatorComponent}
				/>
			) || (!loading && (
				<Text>No services found</Text>
			))}
		</View>
	);
};

const ServiceListSeparatorComponent: FC = () => {
	return (
		<View style={styles.serviceListSeparator} collapsable={false} />
	);
};

interface ServiceProxyProps {
	keyPair: KeyPairStatus;
	serverKey: Uint8Array<ArrayBuffer>;
	serviceName: string;
	streamSplitter: StreamSplitter;
}

const ServiceProxy: FC<ServiceProxyProps> = ({ keyPair, serverKey, serviceName, streamSplitter: splitter }) => {
	type ProxyStatus =
		| {
			status: 'loading',
		}
		| {
			status: 'error',
			error: Error,
		}
		| {
			status: 'available',
		}
		| {
			status: 'active',
			port: number,
		}
	;

	const [proxyStatus, setProxyStatus] = useState<ProxyStatus>({ status: 'loading' });

	useEffect(() => {
		const controller = new AbortController();

		splitter.createStream(async function* (stream, { signal } = {}) {
			yield packetUInt32LE(RPC_IS_PROXY);
			yield packetUInt32LE(serverKey.byteLength);
			yield serverKey;
			const serviceBuf = b4a.from(serviceName) as Uint8Array<ArrayBuffer>;
			yield packetUInt32LE(serviceBuf.byteLength);
			yield serviceBuf;
			for await (const packeter of streamPacketer(stream({ signal: signal ? (AbortSignal as unknown as AbortSignalStatic).any([signal, controller.signal]) : controller.signal }))) {
				const port = await consumeUInt32LE(packeter);
				if (port > 0) {
					setProxyStatus({
						status: 'active',
						port,
					});
				}
				else {
					setProxyStatus({
						status: 'available',
					});
				}
			}
		});

		return () => {
			controller.abort();
		};
	}, [serverKey, serviceName, splitter, setProxyStatus]);

	const startProxy = useCallback(() => {
		if (keyPair.status !== 'valid') {
			return;
		}

		splitter.createStream(async function* (stream, { signal } = {}) {
			yield packetUInt32LE(RPC_PROXY);
			yield packetUInt32LE(keyPair.publicKey.byteLength);
			yield keyPair.publicKey;
			yield packetUInt32LE(keyPair.secretKey.byteLength);
			yield keyPair.secretKey;
			yield packetUInt32LE(serverKey.byteLength);
			yield serverKey;
			const serviceBuf = b4a.from(serviceName) as Uint8Array<ArrayBuffer>;
			yield packetUInt32LE(serviceBuf.byteLength);
			yield serviceBuf;
			for await (const packeter of streamPacketer(stream({ signal }))) {
				const port = await consumeUInt32LE(packeter);
				if (port > 0) {
					setProxyStatus({
						status: 'active',
						port,
					});
				}
			}
		});
	}, [keyPair, serverKey, serviceName, splitter]);

	const stopProxy = useCallback(() => {
		splitter.createStream(async function* (stream, { signal } = {}) {
			yield packetUInt32LE(RPC_UNPROXY);
			yield packetUInt32LE(serverKey.byteLength);
			yield serverKey;
			const serviceBuf = b4a.from(serviceName) as Uint8Array<ArrayBuffer>;
			yield packetUInt32LE(serviceBuf.byteLength);
			yield serviceBuf;
			for await (const _discard of stream({ signal }));
			setProxyStatus({
				status: 'available',
			});
		});
	}, [serverKey, serviceName, splitter]);

	const startBrowser = useCallback(() => {
		if (proxyStatus.status !== 'active') {
			return;
		}

		Linking.openURL(`http://localhost:${ proxyStatus.port }`).catch((error: Error) => {
			Toast.error(error.message);
		});
	}, [proxyStatus]);

	switch (proxyStatus.status) {
		case 'loading': return (
			<ActivityIndicator size="large" />
		);
		case 'error': return (
			<View>
				<Text>Error</Text>
				<Text>{proxyStatus.error.message}</Text>
				<Text>{proxyStatus.error.stack}</Text>
			</View>
		);
		case 'available': return (
			<TextButton
				title="Proxy"
				onPress={startProxy}
				style={styles.centerButton}
				stylePressed={styles.centerButtonPressed}
				styleText={styles.buttonText}
				stylePressedText={styles.buttonPressedText}
			/>
		);
		case 'active': return (<>
			<TextButton
				title="Stop"
				onPress={stopProxy}
				style={styles.centerButton}
				stylePressed={styles.centerButtonPressed}
				styleText={styles.buttonText}
				stylePressedText={styles.buttonPressedText}
			/>
			<TextButton
				title="Launch browser"
				onPress={startBrowser}
				style={styles.centerButton}
				stylePressed={styles.centerButtonPressed}
				styleText={styles.buttonText}
				stylePressedText={styles.buttonPressedText}
			/>
		</>);
	}
};

interface TextButtonProps {
	title: string;
	disabled?: boolean;
	onPress?: () => void;
	style?: StyleProp<ViewStyle>;
	styleText?: StyleProp<TextStyle>;
	stylePressed?: StyleProp<ViewStyle>;
	stylePressedText?: StyleProp<TextStyle>;
	styleDisabled?: StyleProp<ViewStyle>;
	styleDisabledText?: StyleProp<TextStyle>;
}

const TextButton: FC<TextButtonProps> = ({ title, disabled, onPress, style, styleText, stylePressed, stylePressedText, styleDisabled, styleDisabledText}) => {
	if (disabled) {
		return (
			<View style={[style, styleDisabled]}>
				<Text style={[styleText, styleDisabledText]}>{title}</Text>
			</View>
		);
	}
	return (
		<Pressable
			onPress={onPress}
			style={({ pressed }) => pressed ? [style, stylePressed] : [style]}
		>
			{({ pressed }) => (
				<Text style={pressed ? [styleText, stylePressedText] : [styleText]}>{title}</Text>
			)}
		</Pressable>
	);
};

interface Bip39PhraseProps extends Omit<GridProps, 'children'> {
	value: string;
	setValue: React.Dispatch<React.SetStateAction<string>>;
}

interface Bip39LayoutBoundary {
	y: number;
	height: number;
}

const Bip39Phrase: FC<Bip39PhraseProps> = ({ value, setValue, ...props }) => {
	const [boundary, setBoundary] = useState<Bip39LayoutBoundary>({ y: 0, height: 0 });
	const boundaryRef = useRef<View>(null);
	const words = value.split(' ');

	const handleLayout = useCallback(() => {
		boundaryRef.current?.measure((x, y, width, height, pageX, pageY) => {
			setBoundary((prev) => {
				if (pageY !== prev.y || height !== prev.height) {
					return { y: pageY, height };
				}
				return prev;
			});
		});
	}, []);

	return (
		<Grid ref={boundaryRef} collapsable={false} onLayout={handleLayout} {...props}>
			{words.map((word, index) => (
				<Bip39Word
					boundary={boundary}
					value={word}
					setValue={(v: React.SetStateAction<string>) => setValue(words.map((w, i) => i === index ? (typeof v === 'function' ? v(w) : v).replace(/[^a-z]/g, '') : w).join(' '))}
				/>
			))}
		</Grid>
	);
};

interface Bip39WordProps {
	value: string;
	setValue: React.Dispatch<React.SetStateAction<string>>;
	boundary: Bip39LayoutBoundary;
}

const Bip39Word: FC<Bip39WordProps> = ({ value, setValue, boundary, ...props }) => {
	const inputContainer = useRef<View>(null);
	const [floatStyle, setFloatStyle] = useState<StyleProp<ViewStyle>>({ position: 'absolute' });

	useEffect(() => {
		let active = true;
		inputContainer.current?.measure((x, y, width, height, pageX, pageY) => {
			if (!active) {
				return;
			}
			const spaceBelow = boundary.y + boundary.height > pageY + height ? boundary.y + boundary.height - pageY - height : 0;
			const spaceAbove = pageY > boundary.y ? pageY - boundary.y : 0;
			if (spaceBelow > spaceAbove) {
				setFloatStyle({
					position: 'absolute',
					left: 0,
					minWidth: width,
					maxHeight: spaceBelow,
					top: height,
				});
			}
			else {
				setFloatStyle({
					position: 'absolute',
					left: 0,
					minWidth: width,
					maxHeight: spaceAbove,
					bottom: height,
				});
			}
		});
		return () => {
			active = false;
		};
	}, [boundary, value]);

	const data = useMemo<string[]>(() => {
		if (!value.length || wordlist.filter((word) => word === value).length > 0) {
			// FIXME: Instead of hiding the list on exact match, actually track focus and click
			// and hide based on status
			return [];
		}
		return wordlist.filter((word) => word.startsWith(value));
	}, [value]);

	return (
		<View>
			<View ref={inputContainer} collapsable={false}>
				<TextInput value={value} onChangeText={setValue} style={styles.input} { ...props } />
			</View>
			{data.length > 0 && (
				<View collapsable={false} style={[floatStyle, styles.autoCompleteContainer]}>
					<FlatList
						keyboardShouldPersistTaps="always"
						data={data}
						renderItem={({ item }: { item: string }) => (
							<Pressable onPress={() => setValue(item)}>
								<Text style={styles.autoCompleteItem}>{item}</Text>
							</Pressable>
						)}
					/>
				</View>
			)}
		</View>
	);
}

interface Bip39DisplayProps extends Omit<GridProps, 'children'> {
	value: string;
	textStyle?: StyleProp<ViewStyle>;
}

const Bip39Display: FC<Bip39DisplayProps> = ({ value, textStyle, ...props }) => {
	const words = value.split(' ');

	return (
		<Grid {...props}>
			{words.map((word) => (
				<Text style={textStyle}>{word}</Text>
			))}
		</Grid>
	);
};

interface GridProps extends ViewProps, RefAttributes<View> {
	rowStyle?: StyleProp<ViewStyle>;
	cellStyle?: StyleProp<ViewStyle>;
	numColumns: number;
}

const Grid: FC<GridProps> = ({ rowStyle, cellStyle, numColumns, children, ...props }) => {
	let cellIndex = 0;
	let rowIndex = 0;
	const rows: ReactNode[] = [];
	let row: ReactNode[] = [];
	Children.forEach(children, (child) => {
		row.push(
			<View style={[styles.gridCell, cellStyle]} key={`cell${ ++cellIndex }`}>
				{child}
			</View>
		);
		if (row.length >= numColumns) {
			rows.push(
				<View style={[styles.gridRow, rowStyle]} key={`row${ ++rowIndex }`}>
					{row}
				</View>
			);
			row = [];
		}
	});
	if (row.length > 0) {
		while (row.length < numColumns) {
			row.push(
				<View style={[styles.gridCell, cellStyle]} key={`cell${ ++cellIndex }`} />
			);
		}
		rows.push(
			<View style={[styles.gridRow, rowStyle]} key={`row${ ++rowIndex }`}>
				{row}
			</View>
		);
	}
	return (
		<View {...props}>
			{rows}
		</View>
	);
}

const styles = StyleSheet.create({
	container: {
		flex: 1,
	},
	hidden: {
		display: 'none',
	},
	content: {
		flexGrow: 1,
		overflow: 'scroll',
	},
	serverListItem: {
		fontSize: 22,
		padding: 20,
	},
	serverListItemPressed: {
		backgroundColor: '#2196f3',
	},
	serverListSeparator: {
		height: 1,
		backgroundColor: '#000000',
	},
	serviceListItem: {
		fontSize: 22,
		padding: 20,
	},
	serviceListItemPressed: {
		backgroundColor: '#2196f3',
	},
	serviceListSeparator: {
		height: 1,
		backgroundColor: '#000000',
	},
	bipPhrase: {
		flexGrow: 1,
	},
	bipCell: {
		padding: 10,
	},
	bipText: {
		lineHeight: 40,
		paddingLeft: 4,
	},
	gridRow: {
		flexDirection: 'row',
	},
	gridCell: {
		flex: 1,
	},
	editNameContainer: {
		flexDirection: 'row',
		padding: 20,
	},
	serverNameLabel: {
		lineHeight: 40,
		paddingRight: 12,
	},
	serverNameText: {
		lineHeight: 40,
	},
	buttonDrawer: {
		flexDirection: 'row',
		borderColor: 'black',
		borderLeftWidth: 1,
		borderTopWidth: 1,
		borderBottomWidth: 1,
	},
	button: {
		flexGrow: 1,
		borderColor: 'black',
		borderRightWidth: 1,
		backgroundColor: 'white',
		justifyContent: 'center',
	},
	buttonText: {
		textAlign: 'center',
		margin: 20,
		fontSize: 20,
	},
	buttonPressed: {
		backgroundColor: '#2196f3',
	},
	buttonPressedText: {
		color: 'white',
	},
	buttonDisabled: {
		backgroundColor: '#f0f0f0',
	},
	buttonDisabledText: {
		color: '#b0b0b0',
	},
	centerButton: {
		borderColor: 'black',
		borderWidth: 1,
		borderRadius: 40,
		backgroundColor: 'white',
		justifyContent: 'center',
		alignSelf: 'center',
		marginBottom: 20,
	},
	centerButtonPressed: {
		backgroundColor: '#2196f3',
	},
	input: {
		borderColor: '#b9b9b9',
		borderRadius: 1,
		borderWidth: 1,
		backgroundColor: 'white',
		height: 40,
		paddingLeft: 3,
		flex: 1,
	},
	autoCompleteContainer: {
		borderColor: '#b9b9b9',
		borderRadius: 1,
		borderWidth: 1,
		backgroundColor: 'white',
		zIndex: 1,
	},
	autoCompleteItem: {
		fontSize: 15,
		padding: 5,
	},
	floatingLoader: {
		position: 'absolute',
		left: 0,
		right: 0,
		zIndex: 1,
	}
});

export default App;
