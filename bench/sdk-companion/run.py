from pathlib import Path
import asyncio
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import traceback

assert sys.platform == 'linux' and os.getuid() != 0, 'run as an ordinary Linux/WSL user'
stage = Path(__file__).resolve().parent
checkout = stage.parent.parent
assert len(sys.argv) in [2, 3], 'usage: python3 bench/sdk-companion/run.py NEW_OUTPUT_DIR [all|smoke]'
out, scenario = Path(sys.argv[1]).resolve(), sys.argv[2] if len(sys.argv) == 3 else 'all'
assert scenario in ['smoke', 'all'] and not out.is_relative_to(checkout)
package_root = checkout / 'vendor/pi-sdk'
node = shutil.which('node'); assert node, 'Node must be on PATH'
git = lambda *args: subprocess.check_output(['git', '-C', str(checkout), *args], text=True).strip()
sha = lambda file: 'sha256:' + hashlib.sha256(file.read_bytes()).hexdigest()
git_state = lambda: {'head': git('rev-parse', 'HEAD'), 'status': git('status', '--porcelain')}
tree = lambda: {str(file.relative_to(checkout)): sha(file) for directory in ['src', 'dist']
                for file in sorted((checkout / directory).rglob('*')) if file.is_file()}
assert not out.exists()
out.mkdir(parents=True)
build = Path(tempfile.mkdtemp(prefix='pi-sdk-companion-'))
filesystem = subprocess.check_output(['stat', '-f', '-c', '%T', str(build)], text=True).strip()
assert filesystem != 'v9fs', 'use a Linux-native TMPDIR for tool effects and SQLite; DrvFS append is not qualified'
source_names = ['sdk-delivery.mjs', 'owner.mjs', 'store.mjs', 'run.py', 'review.py', 'README.md']
for name in source_names:
    shutil.copyfile(stage / name, out / name)
    if name.endswith('.mjs'): shutil.copyfile(stage / name, build / name)
report = {'base': git('rev-parse', 'HEAD'), 'initialGit': git_state(), 'checkout': str(checkout), 'uid': os.getuid(),
          'workDirectory': str(build), 'filesystem': filesystem,
          'node': subprocess.check_output([node, '--version'], text=True).strip(),
          'implementationTree': tree(), 'scenario': scenario, 'cases': [], 'productionIntegrated': False,
          'scope': 'Packaged companion SDK; real extension/Host; ordinary-user process loss and cooperating owners at stable paths. No native-resource or power-loss claim.',
          'sdk': {}, 'product': {}}
for name in ['extension', 'sdk-tool-binding', 'agent-integration', 'execution-world', 'runtime-engine',
             'stable-json', 'stable-value-hash', 'reuse-store', 'runtime-lifecycle']:
    source = checkout / 'src' / (name + '.ts')
    report['product'][name] = {'sourcePath': str(source), 'source': sha(source), 'compiledPath': str(checkout / 'dist' / (name + '.js')),
                             'compiled': sha(checkout / 'dist' / (name + '.js'))}
report['packageRoot'] = str(package_root)
report['packages'] = json.loads((package_root / 'manifest.json').read_text())
report['sdkVersion'] = report['packages']['source']['version']
report['installedPackages'] = {}
for artifact in report['packages']['artifacts']:
    archive = package_root / artifact['file']; assert sha(archive) == artifact['digest']
    root = checkout / 'node_modules' / artifact['name']
    metadata = json.loads((root / 'package.json').read_text())
    assert metadata['version'] == artifact['version'] == report['sdkVersion']
    files = {}
    with tarfile.open(archive) as packed:
        for member in packed.getmembers():
            if not member.isfile(): continue
            assert member.name.startswith('package/')
            relative = Path(member.name.removeprefix('package/')); assert '..' not in relative.parts and not relative.is_absolute()
            installed = root / relative
            assert installed.read_bytes() == packed.extractfile(member).read(), str(installed)
            files[str(relative)] = sha(installed)
            if str(relative) in ['dist/core/agent-session.js', 'dist/core/session-manager.js', 'dist/core/extensions/runner.js',
                                 'dist/index.js', 'dist/agent.js', 'dist/agent-loop.js']:
                report['sdk'][str(installed)] = sha(installed)
    report['installedPackages'][artifact['name']] = {'root': str(root), 'files': files}
copies = [file for file in checkout.glob('node_modules/**/@earendil-works/*/package.json')
          if json.loads(file.read_text()).get('name') in report['installedPackages']]
assert len(copies) == len(report['packages']['artifacts']), 'duplicate companion package instances'
report['lock'] = sha(checkout / 'package-lock.json')
binding = {'protocol': 'sdk-scope-v1', 'sdkVersion': report['sdkVersion'], 'owner': sha(build / 'owner.mjs'), 'adapter': sha(build / 'sdk-delivery.mjs'),
           'store': sha(build / 'store.mjs'), 'sdk': report['sdk'], 'product': report['product'], 'node': report['node'],
           'tree': report['implementationTree'], 'packages': report['packages']['artifacts'], 'lock': report['lock']}
for folder in [build, out]: (folder / 'binding.json').write_text(json.dumps(binding, sort_keys=True, indent=2))
(out / 'preflight.json').write_text(json.dumps(report, indent=2))
env = {**os.environ, 'PI_OFFLINE': '1'}
runtime = ['nice', '-n', '15', 'taskset', '-c', str(min(os.sched_getaffinity(0))), 'setpriv', '--no-new-privs']

class Peer:
    live = []
    @classmethod
    async def start(cls, action, cwd, mode, label, file=None):
        self = cls(); self.label, self.events, self.cwd = label, [], cwd
        cwd.mkdir(parents=True, exist_ok=True)
        argv = runtime + [node, str(build / 'owner.mjs'), action, str(checkout / 'dist'), str(cwd), mode]
        if file: argv.append(str(file))
        self.process = await asyncio.create_subprocess_exec(*argv, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
        self.errors = asyncio.create_task(self.process.stderr.read()); cls.live.append(self); return self
    async def next(self):
        line = await asyncio.wait_for(self.process.stdout.readline(), 45)
        if not line:
            await self.process.wait(); (out / (self.label + '.stderr')).write_bytes(await self.errors)
            raise AssertionError((self.label, self.process.returncode, (await self.errors).decode()))
        value = json.loads(line); self.events.append(value)
        with (out / (self.label + '.jsonl')).open('a') as file: file.write(json.dumps(value) + '\n')
        return value
    async def send(self):
        self.process.stdin.write(b'continue\n'); await self.process.stdin.drain()
    async def until(self, name, index=None):
        while True:
            event = await self.next(); assert event['event'] != 'result', (name, event)
            if event.get('name') == name and (index is None or event.get('index') == index): return event
            if event['event'] == 'phase': await self.send()
    async def finish(self, held=False):
        if held: await self.send()
        while True:
            event = await self.next()
            if event['event'] == 'result': break
            if event['event'] == 'phase': await self.send()
        await asyncio.wait_for(self.process.wait(), 30)
        (out / (self.label + '.stderr')).write_bytes(await self.errors)
        assert self.process.returncode == 0, (self.label, self.process.returncode, (await self.errors).decode())
        return event
    async def kill(self):
        self.process.kill(); await asyncio.wait_for(self.process.wait(), 30)
        (out / (self.label + '.stderr')).write_bytes(await self.errors)
        assert self.process.returncode == -9
        return {'pid': self.process.pid, 'returncode': self.process.returncode}

def journal(file):
    return [json.loads(line) for line in Path(file).read_text().splitlines()] if Path(file).exists() else []

def snapshot(cwd, file):
    entries, rows, namespace = journal(file), [], None
    database = cwd / 'store/metadata.sqlite'
    if database.exists():
        with sqlite3.connect(f'file:{database}?mode=ro', uri=True) as connection:
            connection.row_factory = sqlite3.Row
            if connection.execute("SELECT 1 FROM sqlite_master WHERE name='qualification_execution_tickets'").fetchone():
                rows = [dict(row) for row in connection.execute('SELECT * FROM qualification_execution_tickets ORDER BY sequence')]
                namespace = dict(connection.execute('SELECT * FROM qualification_ticket_namespace').fetchone())
    return {'journal': entries, 'effects': journal(cwd / 'effects.jsonl'), 'tickets': rows, 'namespace': namespace}

def results(state):
    return [entry for entry in state['journal'] if entry['type'] == 'message' and entry['message']['role'] == 'toolResult']

def rejected(outcome, state, previous):
    assert outcome.get('failure', {}).get('interrupted'), outcome
    assert outcome['hostEntries'] == outcome['hostApplications'] == 0 and not outcome['extensionErrors']
    assert state == previous

def record(case, cwd, file):
    case['sessionFile'] = str(file); case['after'] = snapshot(cwd, file)
    report['cases'].append(case)
    shutil.copyfile(file, out / (case['name'] + '-session.jsonl'))
    if (cwd / 'effects.jsonl').exists(): shutil.copyfile(cwd / 'effects.jsonl', out / (case['name'] + '-effects.jsonl'))
    (out / 'report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps({'recorded': case['name'], 'results': len(results(case['after'])), 'effects': len(case['after']['effects'])}), flush=True)

async def main():
    for name in ['baseline', 'normal']:
        cwd = build / name; done = await (await Peer.start('start', cwd, name, name + '-initial')).finish()
        assert 'failure' not in done and done['hostEntries'] == done['hostApplications'] == done['saved'] == 1, done
        assert all(not entry['message']['isError'] for entry in results(snapshot(cwd, done['sessionFile']))), done
        record({'name': name, 'initial': done}, cwd, done['sessionFile'])
    if scenario == 'smoke': report['passed'] = True; return
    loss = [('issued', 'normal', 'issue_staged', None, 1, False), ('published', 'normal', 'publication_committed', None, 1, False),
            ('prepared', 'normal', 'request_prepared', None, 1, False), ('claimed', 'normal', 'after_claim', None, 0, True),
            ('raw', 'normal', 'raw_ready', None, 0, True), ('finalized', 'normal', 'final_receipt_ready', None, 0, False),
            ('journaled', 'normal', 'sdk_result_saved', None, 0, False), ('tool-failure', 'tool-failure', 'final_receipt_ready', None, 0, False),
            ('terminate', 'terminate', 'final_receipt_ready', None, 0, False),
            ('batch-prefix', 'batch-sequential', 'delivery_acknowledged', 1, 1, False),
            ('batch-finalized', 'batch-sequential', 'final_receipt_ready', 3, 0, False),
            ('parallel-finalized', 'batch-parallel', 'final_receipt_ready', 3, 0, False),
            ('parallel-unknown', 'batch-parallel', 'sdk_result_saved', 1, 0, True)]
    for name, mode, stop, index, applications, poison in loss:
        cwd = build / name; first = await Peer.start('start', cwd, mode, name + '-initial')
        ready = await first.until('session_ready'); file = ready['sessionFile']; await first.send()
        held = await first.until(stop, index); before = snapshot(cwd, file); killed = await first.kill()
        recovered = await (await Peer.start('recover', cwd, mode, name + '-recover', file)).finish()
        after = snapshot(cwd, file)
        case = {'name': name, 'mode': mode, 'stop': held, 'before': before, 'killed': killed, 'recovered': recovered}
        report['cases'].append(case)
        assert recovered['hostApplications'] == recovered['hostEntries'] == applications and not recovered['extensionErrors'], recovered
        if poison:
            assert recovered.get('failure', {}).get('interrupted') and any(row['state'] == 'poisoned' and row['outcome'] is None for row in after['tickets'])
            assert after['effects'] == before['effects'] and results(after) == results(before)
        else:
            assert 'failure' not in recovered, recovered
            assert len(results(after)) == (2 if mode.startswith('batch') else 1)
            assert all(row['acknowledged'] == 1 and row['outcome'] for row in after['tickets'])
            if applications == 0: assert after['effects'] == before['effects']
            again = await (await Peer.start('recover', cwd, mode, name + '-repeat', file)).finish()
            assert 'failure' not in again and again['hostEntries'] == 0 and snapshot(cwd, file) == after, again
            case['repeat'] = again
        report['cases'].pop(); record(case, cwd, file)
    cwd = build / 'live-race'; first = await Peer.start('start', cwd, 'normal', 'live-race-initial')
    ready = await first.until('session_ready'); file = ready['sessionFile']; await first.send(); await first.until('request_prepared')
    before = snapshot(cwd, file); contenders = []
    for action in ['next', 'recover', 'direct']:
        done = await (await Peer.start(action, cwd, 'normal', 'live-race-' + action, file)).finish()
        rejected(done, snapshot(cwd, file), before); contenders.append(done)
    initial = await first.finish(True); assert 'failure' not in initial
    before_capacity = snapshot(cwd, file)
    denied = await (await Peer.start('next', cwd, 'normal', 'capacity-next', file)).finish()
    rejected(denied, snapshot(cwd, file), before_capacity)
    record({'name': 'live-race', 'before': before, 'contenders': contenders, 'initial': initial, 'capacity': denied}, cwd, file)
    for mode in ['close-preparing', 'listener-failure', 'double-admit']:
        cwd = build / mode; done = await (await Peer.start('start', cwd, mode, mode + '-initial')).finish(); file = done['sessionFile']
        state = snapshot(cwd, file)
        if mode == 'double-admit':
            assert 'failure' not in done and done['duplicate']['rejected'] and done['duplicate']['interrupted'] and len(results(state)) == 1
        elif mode == 'close-preparing':
            assert 'failure' not in done and done['closedAt']['effects'] == state['effects'] and done['closedAt']['saved'] == 1
            assert all(item['rejected'] for item in done['afterClose'])
        else:
            assert not results(state) and state['tickets'][0]['state'] == 'poisoned' and state['tickets'][0]['outcome'] is None
            retried = await (await Peer.start('recover', cwd, mode, mode + '-recover', file)).finish()
            rejected(retried, snapshot(cwd, file), state)
        record({'name': mode, 'initial': done}, cwd, file)
    cwd = build / 'implementation-binding'; first = await Peer.start('start', cwd, 'normal', 'implementation-binding-initial')
    ready = await first.until('session_ready'); file = ready['sessionFile']; await first.send(); await first.until('request_prepared')
    killed = await first.kill(); before = snapshot(cwd, file)
    changed = await (await Peer.start('recover', cwd, 'changed-tool', 'implementation-binding-changed', file)).finish()
    rejected(changed, snapshot(cwd, file), before)
    recovered = await (await Peer.start('recover', cwd, 'normal', 'implementation-binding-recover', file)).finish()
    assert 'failure' not in recovered and recovered['hostApplications'] == 1
    record({'name': 'implementation-binding', 'before': before, 'changed': changed, 'recovered': recovered, 'termination': killed}, cwd, file)
    cwd = build / 'request-binding'; first = await (await Peer.start('start', cwd, 'normal', 'request-binding-initial')).finish(); file = first['sessionFile']
    retired = await (await Peer.start('retire', cwd, 'normal', 'request-binding-retire', file)).finish()
    before = snapshot(cwd, file); assert not before['tickets'] and retired['stats']['namespace']['last'] == 1
    retried = await (await Peer.start('recover', cwd, 'normal', 'request-binding-old', file)).finish()
    rejected(retried, snapshot(cwd, file), before)
    second = await (await Peer.start('next', cwd, 'normal', 'request-binding-next', file)).finish()
    after = snapshot(cwd, file); assert 'failure' not in second and second['hostEntries'] == 1 and len(results(after)) == 2
    assert len(after['tickets']) == 1 and after['tickets'][0]['sequence'] == 2
    record({'name': 'request-binding', 'initial': first, 'retired': retired, 'old': retried, 'next': second}, cwd, file)
    changed = journal(file)
    refs = [entry for entry in changed if entry['type'] == 'custom' and entry['customType'] == 'qualification_sdk_ticket']
    refs[0]['data']['context'] = 'sha256:' + '0' * 64
    Path(file).write_text(''.join(json.dumps(entry) + '\n' for entry in changed))
    before = snapshot(cwd, file); corrupt = await (await Peer.start('recover', cwd, 'normal', 'reference-corruption-recover', file)).finish()
    rejected(corrupt, snapshot(cwd, file), before)
    record({'name': 'reference-corruption', 'before': before, 'recovered': corrupt}, cwd, file)
    report['passed'] = True

async def managed():
    try: await main()
    finally:
        for peer in Peer.live:
            if peer.process.returncode is None:
                peer.process.kill(); await asyncio.wait_for(peer.process.wait(), 30)
                (out / (peer.label + '.stderr')).write_bytes(await peer.errors)
        report['processes'] = [{'label': peer.label, 'pid': peer.process.pid, 'exit': peer.process.returncode} for peer in Peer.live]

try: asyncio.run(managed())
except BaseException as error:
    report['error'] = repr(error); report['traceback'] = traceback.format_exc(); raise
finally:
    report['final'] = git_state()
    report['unchanged'] = tree() == report['implementationTree'] and sha(checkout / 'package-lock.json') == report['lock']
    report['sources'] = {name: {'digest': sha(out / name), 'lines': len((out / name).read_text().splitlines())} for name in source_names}
    (out / 'report.json').write_text(json.dumps(report, indent=2))
