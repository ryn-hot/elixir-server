// Fixed read-only follow-up for the one existing synthetic Radicle RID.
// All connections run inside the fresh protected worker, using the worker's
// bounded TLS client, without a proxy, redirect, direct-host fallback or key.
import { createHash } from 'node:crypto';
import { Resolver } from 'node:dns/promises';
import { Agent, request } from 'node:https';
import { fetchPublicSource, sourceFailureCode } from '/opt/elixir-marketplace/src/source-https.mjs';

const control = 'rad:z3gqcJUoA1n9HaHKufZs5FCSGazv5/570a7eb141b6ba001713c46345d79b6fead1ca15/systemd/radicle-node.service';
const rid = 'rad:z46faZkxBmiiBtxoCCUaoSehzFiGH';
const targets = [
    [control, 'a45fe2689221be185e72b6fe38e4dfbc86c96fc86265969facf2df397577eb33'],
    [rid + '/8a3dcef4e9f3efbe88f569437c9f1fa28d6435fd/manifest.yaml', 'b32c8cc77262779c073b1cf8d918a3d52cdaef134720626ca05ade4d445d710b'],
    [rid + '/842277ac588189eccf1c78deac2de665b4942024/manifest.yaml', '36745c11bcc9c65a270b19de7ebca6402f6c3d0956be8a87f0f49bf597a43749'],
];
const resolver = new Resolver({ timeout: 2000, tries: 1 });
resolver.setServers(['1.1.1.1']);
const agent = new Agent({ keepAlive: false, maxSockets: 3, maxTotalSockets: 6, proxyEnv: {} });
const signal = AbortSignal.timeout(20000);
const abort = () => resolver.cancel();
signal.addEventListener('abort', abort, { once: true });
try {
    const hosts = await Promise.all(['iris.radicle.xyz','seed.radicle.dev'].map(async host => {
        const results = await Promise.all(targets.map(async ([path, expected]) => {
            let status = null;
            try {
                const { body } = await fetchPublicSource(
                    { url: `https://${host}/raw/${path}`, maximum_bytes: 65536 },
                    { family: 4, resolver, agent, signal, requestHttps: (options, callback) =>
                        request(options, response => { status = response.statusCode; callback(response); }) },
                    { contentTypes: ['text/plain','application/octet-stream','application/yaml','text/yaml'] },
                );
                return { status, digest_matches: createHash('sha256').update(body).digest('hex') === expected };
            } catch (error) {
                return { status, failure: sourceFailureCode(error) };
            }
        }));
        return { host, control: results[0], v1: results[1], v2: results[2] };
    }));
    console.log(JSON.stringify({ stage: 'iris_protected_public_read', rid, hosts }));
} finally {
    signal.removeEventListener('abort', abort);
    resolver.cancel();
    agent.destroy();
}
