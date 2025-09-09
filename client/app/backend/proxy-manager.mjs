import '../src/polyfills.mjs';
import {
	createServer,
} from 'bare-tcp';
import {
	RPC_PROXY,
} from './rpc-commands.mjs';
import {
	packetUInt32LE,
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
	return created.address().port;
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
	return proxy.address().port;
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
	proxy.close();
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
		proxy.close();
	}
	activeProxies.delete(server64)
}

async function createProxy(node, keyPair, serverKey, serviceName) {
	const server = createServer();
	server.on('connection', (socket) => {
		const remote = node.connect(serverKey, { keyPair });
		// These writes can be done "blind" because streamx will queue and deliver them
		remote.write(packetUInt32LE(RPC_PROXY));
		remote.write(packetUInt32LE(serviceName.byteLength));
		remote.write(serviceName);
		// Now plumb them together
		// Log errors, but don't do anything about them
		// That's between the server and the connecting application. This app only proxies
		remote.pipe(socket, (err) => {
			if (err) {
				console.log(err);
			}
		});
		socket.pipe(remote, (err) => {
			if (err) {
				console.log(err);
			}
		});
	});
	// Listen only on localhost, on a random port
	await new Promise((resolve) => {
		server.listen('127.0.0.1', resolve);
	});
	return server;
}
