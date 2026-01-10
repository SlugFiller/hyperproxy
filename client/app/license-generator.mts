import {
	scanDependencies,
	writeAboutLibrariesNPMOutput,
} from '@callstack/licenses/node';
import type {
	Types,
} from '@callstack/licenses';
import {
	chdir,
} from 'node:process';

const optionsFactory: Types.ScanPackageOptionsFactory = ({ isRoot }) => ({
	includeDevDependencies: isRoot,
	includeTransitiveDependencies: true,
	includeOptionalDependencies: true,
});

const licensesMain = scanDependencies('/usr/src/app/package.json', optionsFactory);
chdir('./backend');
const licensesBackend = scanDependencies('/usr/src/app/backend/package.json', optionsFactory);
chdir('..');

const licenses = {
	...licensesBackend,
	...licensesMain,
};

writeAboutLibrariesNPMOutput(licenses, '/usr/src/app/android');
