/**
 * Sample React Native App
 * https://github.com/facebook/react-native
 *
 * @format
 */

import {
	wordlist
} from '@scure/bip39/wordlists/english';
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
	KeyboardAvoidingView,
	KeyboardProvider,
} from 'react-native-keyboard-controller';
import {
	SafeAreaProvider,
	SafeAreaView,
} from 'react-native-safe-area-context';

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

const AppContent: FC = () => {
	const [phrase, setPhrase] = useState<string>(' '.repeat(23));

	return (
		<SafeAreaView style={styles.container}>
			<KeyboardAvoidingView behavior={"height"} style={styles.container}>
				<Bip39Phrase
					value={phrase}
					setValue={setPhrase}
					numColumns={4}
					cellStyle={styles.bipCell}
				/>
			</KeyboardAvoidingView>
		</SafeAreaView>
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
			const spaceBelow = boundary.y + boundary.height > y + pageY ? boundary.y + boundary.height - y - pageY : 0;
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
	bipCell: {
		padding: 10,
	},
	gridRow: {
		flexDirection: 'row',
	},
	gridCell: {
		flex: 1,
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
