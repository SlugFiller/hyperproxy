import {
	spawn,
} from 'node:child_process';
import {
	watch,
} from 'node:fs/promises';
import {
	fileURLToPath,
} from 'node:url';
const env = {...process.env, 'METRO_PORT': process.argv[3] || '8081' };
const watchAbort = new AbortController();
async function exec(command, ...args) {
	const child = spawn(command, args, {
		stdio: 'inherit',
		env,
	});
	function sigint() {
		child.kill('SIGINT');
	};
	try {
		process.on('SIGINT', sigint);
		return await new Promise((resolve) => {
			child.on('exit', (code) => resolve(code === 0));
		});
	}
	finally {
		process.off('SIGINT', sigint);
	}
}
function cycleWait() {
	let finalAborted = false;
	let current = {
		controller: new AbortController(),
		used: false,
	};
	return {
		abort: () => {
			current.controller.abort();
			if (current.used && !finalAborted) {
				current = {
					controller: new AbortController(),
					used: false,
				};
			}
		},
		finalAbort: () => {
			finalAborted = true;
			current.controller.abort();
		},
		wait: () => {
			return new Promise((res) => {
				const hold = current;
				hold.used = true;
				const signal = hold.controller.signal;
				if (signal.aborted) {
					if (!finalAborted) {
						current = {
							controller: new AbortController(),
							used: false,
						};
					}
					res(!finalAborted);
					return;
				}
				signal.addEventListener('abort', () => {
					res(!finalAborted);
				});
			});
		}
	};
}
function debounceTimer() {
	let current = null;
	let waiter = cycleWait();
	if (watchAbort.signal.aborted) {
		waiter.finalAbort();
	}
	else {
		watchAbort.signal.addEventListener('abort', () => {
			waiter.finalAbort();
			if (current) {
				clearTimeout(current.timeout);
				current = null;
			}
		});
	}
	return {
		debounce: () => {
			if (watchAbort.signal.aborted) {
				return;
			}
			if (current) {
				clearTimeout(current.timeout);
			}
			const next = {
				timeout: null,
			};
			current = next;
			next.timeout = setTimeout(() => {
				if (watchAbort.signal.aborted) {
					return;
				}
				if (next === current) {
					waiter.abort();
				}
			}, 1000);
		},
		wait: () => {
			return waiter.wait();
		},
	};
}
async function deploy() {
	if (await exec('docker-compose', 'cp', './app', 'debug:/usr/src') &&
		await exec('docker-compose', 'exec', 'debug', 'npx', 'tsc', '-p', 'backend') &&
		await exec('docker-compose', 'exec', 'debug', 'npx', 'bare-pack', '--host', 'android', '--linked', '--out', './backend.bundle.mjs', 'backend/dist/backend/backend.js') &&
		await exec('docker-compose', 'exec', 'debug', 'npx', 'tsc', '--noEmit') &&
		await exec('docker-compose', 'exec', 'debug', 'npm', 'run', 'lint')) {
		console.log('Deployed');
		return true;
	}
	return false;
}
const timer = debounceTimer();
const watcher = watch(fileURLToPath(new URL('app', import.meta.url)), {
	signal: watchAbort.signal,
	recursive: true,
});
const reverse = await exec('adb', 'reverse', 'tcp:8081', 'tcp:' + (process.argv[3] || '8081'));
await exec('docker-compose', '--progress=plain', 'up', '-d', '--build', 'debug') &&
await deploy() && await Promise.all([
	(async () => {
		await exec('docker-compose', 'exec', 'debug', 'npm', 'run', 'start');
		watchAbort.abort();
	})(),
	(async () => {
		try {
			for await (const { filename } of watcher) {
				for (const suffix of ['.js', '.ts', '.mjs', '.mts', '.jsx', '.tsx']) {
					if (filename.endsWith(suffix)) {
						timer.debounce();
						break;
					}
				}
			}
		} catch (err) {
			if (err.name !== 'AbortError') {
				console.log(err);
			}
		}
	})(),
	(async () => {
		while (await timer.wait()) {
			await deploy();
		}
	})(),
]);
watchAbort.abort();
await exec('docker-compose', 'down', 'debug');
if (reverse) {
	await exec('adb', 'reverse', '--remove', 'tcp:8081');
}
