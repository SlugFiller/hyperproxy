/*
 * SPDX-License-Identifier: 0BSD
 *
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

import {
	type Types,
} from '@callstack/licenses';
import {
	scanDependencies,
	writeAboutLibrariesNPMOutput,
} from '@callstack/licenses/node';
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
