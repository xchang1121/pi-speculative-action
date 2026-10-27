from pathlib import Path
import hashlib
import json
import sys

out = Path(sys.argv[1]); report = json.loads((out / 'report.json').read_text())
sha = lambda data: 'sha256:' + hashlib.sha256(data).hexdigest()
read = lambda file: [json.loads(line) for line in Path(file).read_text().splitlines()] if Path(file).exists() else []
assert report['passed'] and report['final'] == report['initialGit'] and report['unchanged']
assert report['filesystem'] != 'v9fs'
assert not report['productionIntegrated']
if 'source' in report['packages']:
    assert report['packages']['source']['version'] == report['sdkVersion'] == '0.84.1-speculative.1'
    assert report['packages']['source']['commit'] == '53fa77ccd8a279eb87e92294ef3687b03ff80112'
else: assert report['packages']['qualificationOnly']
assert all(peer['exit'] in [0, -9] for peer in report['processes'])
for artifact in report['packages']['artifacts']:
    assert sha((Path(report['packageRoot']) / artifact['file']).read_bytes()) == artifact['digest']
for name, data in report['sources'].items():
    file = out / name; assert sha(file.read_bytes()) == data['digest']
    assert len(file.read_text().splitlines()) == data['lines']
for name, digest in report['sdk'].items(): assert sha(Path(name).read_bytes()) == digest
for package in report['installedPackages'].values():
    for file, digest in package['files'].items(): assert sha((Path(package['root']) / file).read_bytes()) == digest
for file, digest in report['implementationTree'].items(): assert sha((Path(report['checkout']) / file).read_bytes()) == digest
for data in report['product'].values():
    assert sha(Path(data['sourcePath']).read_bytes()) == data['source']
    assert sha(Path(data['compiledPath']).read_bytes()) == data['compiled']
binding = json.loads((out / 'binding.json').read_text())
assert binding['sdk'] == report['sdk'] and binding['product'] == report['product'] and binding['sdkVersion'] == report['sdkVersion']
assert binding['owner'] == report['sources']['owner.mjs']['digest'] and binding['adapter'] == report['sources']['sdk-delivery.mjs']['digest']
assert binding['tree'] == report['implementationTree'] and binding['lock'] == report['lock'] and binding['node'] == report['node']
assert binding['packages'] == report['packages']['artifacts']
for peer in report['processes']:
    events = read(out / (peer['label'] + '.jsonl'))
    deployment, = [event for event in events if event['name'] == 'deployment']
    assert deployment['uid'] == report['uid'] != 0
    fields = {line.split(':', 1)[0]: line.split(':', 1)[1].strip() for line in deployment['status']}
    assert int(fields['CapEff'], 16) == int(fields['CapPrm'], 16) == 0 and fields['NoNewPrivs'] == '1'
    if peer['exit'] == 0:
        result, = [event for event in events if event['event'] == 'result']
        assert result['extensionErrors'] == []
        assert result['hostEntries'] == len([event for event in events if event['name'] == 'host_entry'])
        assert result['hostApplications'] == len([event for event in events if event['name'] == 'host_apply'])
checked = []
for case in report['cases']:
    name, state = case['name'], case['after']
    entries = read(out / (name + '-session.jsonl')); assert entries == state['journal']
    assert read(out / (name + '-effects.jsonl')) == state['effects']
    messages = [entry for entry in entries if entry['type'] == 'message' and entry['message']['role'] == 'toolResult']
    refs = [entry['data'] for entry in entries if entry['type'] == 'custom' and entry['customType'] == 'qualification_sdk_ticket']
    by_id = {entry['id']: entry for entry in entries if 'id' in entry}
    for row in state['tickets']:
        ref, = [ref for ref in refs if ref['execution'].endswith(':' + str(row['sequence']))]
        call = by_id[ref['entry']]['message']['content'][ref['index']]
        assert call['id'] == 'sdk-call-1' and ref['index'] in [1, 3]
        assert row['context'] == ref['context'] and row['record'] == ref['record']
        payload = {'session': ref['session'], 'entry': ref['entry'], 'index': ref['index'], 'call': call}
        assert sha(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()) == row['context']
        if row['outcome']:
            assert sha(row['outcome'].encode()) == row['receipt']
            receipt = json.loads(row['outcome'])
            assert receipt['schema'] == 2 and receipt['execution'] == ref['execution'] and receipt['context'] == row['context']
            assert receipt['disposition'] == row['state'] and type(receipt['result']['terminate']) is bool
            assert receipt['result']['terminate'] == (name == 'terminate')
            if not receipt['result']['message']['isError']:
                assert receipt['result']['message']['details']['call'] == call['arguments']['text']
            matched = [message for message in messages if message['message'] == receipt['result']['message']]
            assert len(matched) == 1 and row['acknowledged'] == 1
        else: assert row['state'] == 'poisoned' and row['acknowledged'] == 0
    for message in messages:
        body = message['message']; text = body['content'][0]['text']
        if body['isError']:
            assert name == 'tool-failure' and text == 'final:hook:owned tool failure'
        else:
            assert text == f"final:hook:raw:{body['details']['call']}:{body['details']['sourcePID']}"
        assert body['details']['hookPID'] == body['details']['finalPID']
        if not body['isError']: assert body['details']['sourcePID'] == body['details']['hookPID']
    if 'killed' in case:
        assert case['killed']['returncode'] == -9
        restored = case['recovered']; events = read(out / (name + '-recover.jsonl'))
        assert not any(event['name'] in ['input', 'before_agent_start'] for event in events)
        if name in ['issued', 'published', 'prepared', 'batch-prefix']:
            assert restored['hostApplications'] == 1 and 'failure' not in restored
            assert sum(event['name'] == 'tool' for event in events) == 1
        else:
            assert restored['hostApplications'] == 0 and state['effects'] == case['before']['effects']
            assert not any(event['name'] in ['tool_execution_start', 'prepare_arguments', 'tool_call', 'tool', 'tool_result', 'message_end'] for event in events)
        if name in ['claimed', 'raw', 'parallel-unknown']:
            assert restored['failure']['interrupted']
            assert len(messages) == (1 if name == 'parallel-unknown' else 0)
        else:
            assert 'failure' not in restored and 'failure' not in case['repeat'] and case['repeat']['hostEntries'] == 0
            repeated = read(out / (name + '-repeat.jsonl'))
            assert not any(event['name'] in ['input', 'before_agent_start', 'tool', 'tool_result', 'message_end'] for event in repeated)
        if name == 'prepared':
            assert [event['name'] for event in case['before']['effects']] == ['input', 'before_agent_start']
        if name == 'journaled': assert len([e for e in case['before']['journal'] if e.get('message', {}).get('role') == 'toolResult']) == 1
        if name == 'batch-prefix':
            source_pids = [entry['message']['details']['sourcePID'] for entry in messages]
            assert source_pids == [case['killed']['pid'], restored['pid']]
        if name.startswith('parallel-'): assert sum(effect['name'] == 'tool' for effect in state['effects']) == 2
    elif name in ['normal', 'baseline']:
        assert [event['name'] for event in state['effects']] == ['input', 'before_agent_start', 'tool_execution_start', 'prepare_arguments', 'tool_call', 'tool', 'tool_result', 'message_end']
        assert case['initial']['hostApplications'] == len(messages) == 1
    elif name == 'live-race':
        assert [effect['name'] for effect in case['before']['effects']] == ['input', 'before_agent_start']
        for done in case['contenders'] + [case['capacity']]:
            assert done['failure']['interrupted'] and done['hostEntries'] == 0
            assert not any(effect['pid'] == done['pid'] for effect in state['effects'])
        assert len(messages) == 1
    elif name == 'close-preparing':
        done = case['initial']; assert done['closedAt']['effects'] == state['effects'] and done['closedAt']['saved'] == 1
        assert all(entry['rejected'] for entry in done['afterClose'])
        events = read(out / (name + '-initial.jsonl')); closed, = [i for i, e in enumerate(events) if e['name'] == 'owner_closed']
        assert not any(event['name'] in ['tool', 'tool_result', 'message_end'] for event in events[closed + 1:])
    elif name == 'double-admit':
        assert case['initial']['duplicate']['rejected'] and case['initial']['duplicate']['interrupted']
        assert len(messages) == 1 and [effect['name'] for effect in state['effects']].count('input') == 1
    elif name == 'implementation-binding':
        assert case['changed']['failure']['interrupted'] and case['changed']['hostEntries'] == 0
        assert case['recovered']['hostEntries'] == 1 and state['namespace']['last'] == 1 and len(messages) == 1
    elif name == 'listener-failure': assert not messages and state['tickets'][0]['state'] == 'poisoned'
    elif name == 'request-binding':
        assert len(refs) == len(messages) == 2 and refs[0]['entry'] != refs[1]['entry'] and refs[0]['context'] != refs[1]['context']
        assert case['old']['failure']['interrupted'] and case['next']['hostEntries'] == 1 and state['namespace']['last'] == 2
    elif name == 'reference-corruption':
        assert refs[0]['context'] == 'sha256:' + '0' * 64 and case['recovered']['failure']['interrupted'] and state == case['before']
    else: raise AssertionError(name)
    checked.append({'name': name, 'results': len(messages), 'toolEffects': sum(effect['name'] == 'tool' for effect in state['effects'])})
expected = {'baseline', 'normal'}
if report['scenario'] == 'all': expected |= {'issued', 'published', 'prepared', 'claimed', 'raw', 'finalized', 'journaled', 'tool-failure',
    'batch-prefix', 'batch-finalized', 'parallel-finalized', 'parallel-unknown', 'live-race', 'close-preparing', 'listener-failure', 'request-binding', 'reference-corruption',
    'terminate', 'double-admit', 'implementation-binding'}
assert {case['name'] for case in report['cases']} == expected
assert report['sources']['sdk-delivery.mjs']['lines'] <= 260 and report['sources']['store.mjs']['lines'] - 168 <= 120
result = {'passed': True, 'checked': checked, 'scope': report['scope'], 'productionIntegrated': False,
          'sourceLines': {name: data['lines'] for name, data in report['sources'].items()}}
(out / 'review.json').write_text(json.dumps(result, indent=2)); print(json.dumps(result))
