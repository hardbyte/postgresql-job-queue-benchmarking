import argparse
import json
import statistics
from pathlib import Path

parser = argparse.ArgumentParser(description='Report three W128 pairs, including the original campaign.')
parser.add_argument('initial', type=Path)
parser.add_argument('repeats', type=Path)
args = parser.parse_args()
metrics = ['median_throughput_per_s', 'median_end_to_end_p99_ms', 'median_queue_depth']
pairs = []
for pair in (1,2,3):
    builds = {}
    for label in ('baseline','candidate'):
        cell = args.initial / f'sat-w128-{label}' if pair == 1 else args.repeats / f'pair{pair}-{label}'
        phase = json.loads((cell / 'summary.json').read_text())['systems']['awa']['phases']['clean']
        builds[label] = {key: phase[key] for key in metrics}
    delta = {key: 100 * (builds['candidate'][key] / builds['baseline'][key] - 1) for key in metrics}
    pairs.append({'pair': pair, 'builds': builds, 'candidate_delta_percent': delta})
lines = ['# W128 saturation repeats', '',
    'Same archived main (`49b1a77`) and #481 (`a8e7e63`) executables as the initial campaign, both using the pinned upstream SQLx socket fix. Fresh PostgreSQL 18.3 per cell; W128, depth target 4,000, offered ceiling 50,000/s, 60s warmup and 180s measurement. Pair order: baseline/candidate, candidate/baseline, baseline/candidate. No local compilation or other benchmark ran concurrently.', '',
    'Rates are handler completions before database completion-batch commit. Latency is the median of rolling 30-second p99 samples at five-second cadence, not a job-level aggregate p99.', '',
    '| Pair | Build | Complete/s | E2E p99 ms | Queue depth |',
    '| --- | --- | ---: | ---: | ---: |']
for pair in pairs:
    for label, data in pair['builds'].items():
        lines.append(f"| {pair['pair']} | {label} | " + ' | '.join(f'{data[k]:,.1f}' for k in metrics) + ' |')
lines += ['', '| Pair | Throughput delta | Latency delta |', '| --- | ---: | ---: |']
for pair in pairs:
    d=pair['candidate_delta_percent']
    lines.append(f"| {pair['pair']} | {d[metrics[0]]:+.1f}% | {d[metrics[1]]:+.1f}% |")
lines += ['', 'Across the three pairs, median candidate throughput delta is '
    f"{statistics.median(p['candidate_delta_percent'][metrics[0]] for p in pairs):+.1f}% and median latency delta is "
    f"{statistics.median(p['candidate_delta_percent'][metrics[1]] for p in pairs):+.1f}%.", '',
    'Three sequential pairs on one host describe observed variation; they do not establish statistical significance. These transport-patched measurements do not approve the unpatched shipping SQLx dependency.', '']
(args.repeats / 'comparison.json').write_text(json.dumps(pairs, indent=2) + '\n')
(args.repeats / 'SUMMARY.md').write_text('\n'.join(lines))
print('\n'.join(lines))
