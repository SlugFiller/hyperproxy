import '../src/polyfills.mjs';
import {
	createServer,
} from 'bare-tcp';
import {
	RPC_PROXY,
} from './rpc-commands.mjs';
import {
	StreamSplitter,
	combineStreams,
	packetUInt32LE,
	readerFromNodeStream,
	runStream,
	writerFromNodeStream,
} from '../src/parse-utils.mjs';

const activeProxies = new Map();

export async function addOrGetProxy(node, keyPair, serverKey, serviceName) {
	const server64 = serverKey.toString('base64');
	const service64 = serviceName.toString('base64');
	if (!activeProxies.has(server64)) {
		activeProxies.set(server64, new Map());
	}
	const services = activeProxies.get(server64)
	const prev = services.get(service64);
	if (prev) {
		return prev.address().port;
	}
	const created = await createProxy(node, keyPair, serverKey, serviceName);
	services.set(service64, created);
	return created[0].address().port;
}

export function getProxy(serverKey, serviceName) {
	const server64 = serverKey.toString('base64');
	const service64 = serviceName.toString('base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return 0;
	}
	const proxy = services.get(service64);
	if (!proxy) {
		return 0;
	}
	return proxy[0].address().port;
}

export function removeProxy(serverKey, serviceName) {
	const server64 = serverKey.toString('base64');
	const service64 = serviceName.toString('base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return;
	}
	const proxy = services.get(service64);
	if (!proxy) {
		return;
	}
	proxy[0].close();
	proxy[1].destroy();
	services.delete(service64);
	if (services.size < 1) {
		activeProxies.delete(server64)
	}
}

export function removeAllProxies(serverKey) {
	const server64 = serverKey.toString('base64');
	const services = activeProxies.get(server64)
	if (!services) {
		return;
	}
	for (const proxy of services.values()) {
		proxy[0].close();
		proxy[1].destroy();
	}
	activeProxies.delete(server64)
}

async function createProxy(node, keyPair, serverKey, serviceName) {
	const server = createServer();
	const remote = node.connect(serverKey, { keyPair });
	// These writes can be done "blind" because streamx will queue and deliver them
	remote.write(packetUInt32LE(RPC_PROXY));
	remote.write(packetUInt32LE(serviceName.byteLength));
	remote.write(serviceName);
	const splitter = new StreamSplitter();
	runStream(combineStreams(
		readerFromNodeStream(remote),
		splitter.split,
		writerFromNodeStream(remote)
	)).catch(() => {
		removeProxy(serverKey, serviceName);
	});
	server.on('connection', (socket) => {
		splitter.createStream(async function* (source, { signal } = {}) {
			const controller = new AbortController();
			try {
				const writeTask = runStream(combineStreams(
					async function* () {
						yield* source({ signal });
					},
					writerFromNodeStream(socket)
				)).catch((error) => {
					controller.abort(error);
				});
				yield* readerFromNodeStream(socket)(null, { signal: controller.signal });
				await writeTask;
			}
			finally {
				socket.destroy();
			}
		});
	});
	// Listen only on localhost, on a random port
	await new Promise((resolve) => {
		server.listen('127.0.0.1', resolve);
	});
	return [server, remote];
}
