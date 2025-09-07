import {
	TurboModuleRegistry,
} from 'react-native';
import type {
	TurboModule,
} from 'react-native';

export interface Spec extends TurboModule {
	startService(): void;

	readMain(): Promise<string>;
	writeMain(buffer: string): Promise<void>;
	readWorklet(): Promise<string>;
	writeWorklet(buffer: string): Promise<void>;
}

export default TurboModuleRegistry.getEnforcing<Spec>(
	'ForegroundWorklet',
);
