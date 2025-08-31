/**
 * Sample React Native App
 * https://github.com/facebook/react-native
 *
 * @format
 */

import {
	entropyToMnemonic,
} from '@scure/bip39';
import {
	wordlist,
} from '@scure/bip39/wordlists/english';
import b4a from 'b4a';
import RPC from 'bare-rpc';
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
} from 'react';
import {
	ActivityIndicator,
	FlatList,
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
	ViewStyle,
} from 'react-native';
import {
	Worklet,
} from 'react-native-bare-kit';
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
import backendBundle from './backend.bundle.mjs'
import {
	RPC_KEYGEN,
} from './backend/rpc-commands.mjs';

enableSimpleNullHandling();

const db = open({ name: 'config.sqlite' });
db.execute(`CREATE TABLE IF NOT EXISTS keypair(
	publicKey BLOB,
	secretKey BLOB
)`);

function toBuffer(typedarray: Uint8Array): ArrayBuffer {
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
	publicKey: Uint8Array;
	secretKey: Uint8Array;
}

type KeyPairStatus = 
	| {
		status: 'loading',
	}
	| {
		status: 'valid',
		value: KeyPair,
	}
	| {
		status: 'error',
		error: KeyPair,
	}
;

const AppContent: FC = () => {
	const [phrase, setPhrase] = useState<string>(' '.repeat(23));
	const [keyPair, setKeyPair] = useState<KeyPairStatus>({ status: 'loading' });
	const [showKey, setShowKey] = useState<boolean>(false);

	const worklet = useMemo(() => {
		return new Worklet();
	}, []);

	useEffect(() => {
		worklet.start('/backend.bundle', backendBundle);

		return () => {
			worklet.IPC.destroy();
		};
	}, [worklet]);

	const rpc = useMemo(() => {
		return new RPC(worklet.IPC);
	}, [worklet]);

	useEffect(() => {
		// Attempt to load key from storage
		do {
			const keyPairRow = db.execute(`SELECT publicKey, secretKey FROM keypair LIMIT 1`).rows?.item(0);
			if (!keyPairRow) {
				break;
			}
			setKeyPair({
				status: 'valid',
				value: {
					publicKey: new Uint8Array(keyPairRow.publicKey),
					secretKey: new Uint8Array(keyPairRow.secretKey),
				},
			});
			return;
		}
		while (false);

		// Failed. Generate new key
		const req = rpc.request(RPC_KEYGEN);
		req.send(b4a.alloc(0));
		req.reply().then((keyBundle: Uint8Array) => {
			const pubKeyLen = b4a.readUInt32LE(keyBundle);
			const newKeyPair = {
				publicKey: keyBundle.subarray(4, 4 + pubKeyLen),
				secretKey: keyBundle.subarray(4 + pubKeyLen, keyBundle.byteLength),
			};
			setKeyPair({
				status: 'valid',
				value: newKeyPair,
			});
			db.execute(`INSERT INTO keypair(publicKey, secretKey) SELECT ?, ?
				FROM (SELECT 1 t) t LEFT JOIN keypair e ON (1=1) WHERE e.ROWID IS NULL`,
				[
					toBuffer(newKeyPair.publicKey),
					toBuffer(newKeyPair.secretKey),
				]
			);
		}, (error: Error) => {
			setKeyPair({
				status: 'error',
				error,
			});
		});
	}, [rpc]);

	return (
		<SafeAreaView style={styles.container}>
			<KeyboardAvoidingView behavior={"height"} style={styles.container}>
				{showKey ? (<>
					{(() => { switch (keyPair.status) {
						case 'loading': return (
							<ActivityIndicator />
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
								value={entropyToMnemonic(keyPair.value.publicKey, wordlist)}
								numColumns={4}
								style={styles.bipPhrase}
								cellStyle={styles.bipCell}
								textStyle={styles.bipText}
							/>
						);
					} })()}
				</>) : (
					<Bip39Phrase
						value={phrase}
						setValue={setPhrase}
						numColumns={4}
						style={styles.bipPhrase}
						cellStyle={styles.bipCell}
					/>
				)}
				<View style={styles.buttonDrawer}>
					{showKey ? (
						<TextButton
							title="Back"
							onPress={() => setShowKey(false)}
							style={styles.button}
							stylePressed={styles.buttonPressed}
							styleText={styles.buttonText}
							stylePressedText={styles.buttonPressedText}
						/>
					) : (
						<TextButton
							title="Show my key"
							onPress={() => setShowKey(true)}
							style={styles.button}
							stylePressed={styles.buttonPressed}
							styleText={styles.buttonText}
							stylePressedText={styles.buttonPressedText}
						/>
					)}
				</View>
			</KeyboardAvoidingView>
		</SafeAreaView>
	);
};

interface TextButtonProps {
	title: string;
	onPress: () => void;
	style: StyleProp<ViewStyle>;
	styleText: StyleProp<ViewStyle>;
	stylePressed: StyleProp<ViewStyle>;
	stylePressedText: StyleProp<ViewStyle>;
}

const TextButton: FC<TextButtonProps> = ({ title, onPress, style, styleText, stylePressed, stylePressedText}) => {
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

interface Bip39PhraseProps {
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
		<Grid ref={boundaryRef} onLayout={handleLayout} {...props}>
			{words.map((word, index) => (
				<Bip39Word
					boundary={boundary}
					value={word}
					setValue={(v: string) => setValue(words.map((w, i) => i === index ? v : w).join(' '))}
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
		if (!value.length) {
			return [];
		}
		const ret: string[] = wordlist.filter((word) => word.startsWith(value));
		if (ret.length === 1 && ret[0] === value) {
			return [];
		}
		return ret;
	}, [value]);

	return (
		<View>
			<View ref={inputContainer} collapsable={false}>
				<TextInput value={value} onChangeText={setValue} style={styles.input} { ...props } />
			</View>
			{data.length > 0 && (
				<View collapsable={false} style={[floatStyle, styles.autoCompleteContainer]}>
					<FlatList
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

interface Bip39DisplayProps {
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

interface GridProps {
	rowStyle?: StyleProp<ViewStyle>;
	cellStyle?: StyleProp<ViewStyle>;
	numColumns: number;
	children: ReactNode;
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
	bipPhrase: {
		flex: 1,
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
	buttonDrawer: {
		flexDirection: 'row',
		borderColor: 'black',
		borderLeftWidth: 1,
		borderTopWidth: 1,
		borderBottomWidth: 1,
	},
	button: {
		flex: 1,
		borderColor: 'black',
		borderRightWidth: 1,
	},
	buttonText: {
		textAlign: 'center',
		margin: 20,
		fontSize: 20,
	},
	buttonPressed: {
		backgroundColor: '#2196F3',
	},
	buttonPressedText: {
		color: 'white',
	},
	input: {
		borderColor: '#b9b9b9',
		borderRadius: 1,
		borderWidth: 1,
		backgroundColor: 'white',
		height: 40,
		paddingLeft: 3,
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
});

export default App;
