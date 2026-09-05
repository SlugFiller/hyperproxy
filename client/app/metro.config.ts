import { getDefaultConfig, mergeConfig } from '@react-native/metro-config';
import type { MetroConfig } from '@react-native/metro-config';

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import('@react-native/metro-config').MetroConfig}
 */
const config: MetroConfig = {};

export default mergeConfig(getDefaultConfig((import.meta as unknown as { dirname: string }).dirname), config);
