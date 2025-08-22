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
	useMemo,
	useState,
} from 'react';
import {
	Pressable,
	StatusBar,
	StyleSheet,
	Text,
	useColorScheme,
} from 'react-native';
import AutoComplete from 'react-native-autocomplete-input';
import {
	SafeAreaProvider,
	SafeAreaView,
} from 'react-native-safe-area-context';

function App() {
	const isDarkMode = useColorScheme() === 'dark';

	return (
		<SafeAreaProvider>
			<StatusBar barStyle={isDarkMode ? 'light-content' : 'dark-content'} />
			<AppContent />
		</SafeAreaProvider>
	);
}

function AppContent() {
	const [word, setWord] = useState<string>('');

	return (
		<SafeAreaView style={styles.container}>
			<Bip39Word
				value={word}
				setValue={setWord}
			/>
		</SafeAreaView>
	);
}

function Bip39Word({ value, setValue, ...props }: { value: string, setValue: React.Dispatch<React.SetStateAction<string>> }) {
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
		<AutoComplete
			data={data}
			value={value}
			onChangeText={setValue}
			flatListProps={{
				renderItem: ({ item }: { item: string }) => (
					<Pressable onPress={() => setValue(item)}>
						<Text style={styles.autoCompleteItem}>{item}</Text>
					</Pressable>
				),
			}}
			{ ...props }
		/>
	);
}

const styles = StyleSheet.create({
	container: {
		flex: 1,
	},
	autoCompleteItem: {
		fontSize: 15,
		padding: 5,
	},
});

export default App;
