#!/usr/bin/env python3
"""Render a comparable figure set from a completed AWA campaign's portable evidence."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import StrMethodFormatter

COLORS = {'baseline': '#4E79A7', 'candidate': '#F28E2B'}
STYLES = {'baseline': '--', 'candidate': '-'}


def render(root: Path) -> None:
    campaign = json.loads((root / 'campaign.json').read_text())
    if campaign['status'] != 'complete':
        raise ValueError('Publish campaign plots only after the campaign completes')
    labels = {b: f"{b.title()} · {campaign['builds'][b]['git_sha'][:7]}" for b in COLORS}
    phases = {c['name']: json.loads((root / c['path'] / 'summary.json').read_text())['systems']['awa']['phases']
              for c in campaign['cells']}
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.titleweight': 'bold', 'svg.fonttype': 'none',
                         'svg.hashsalt': 'awa-campaign', 'savefig.facecolor': 'white'})
    out = root / 'plots'
    out.mkdir(exist_ok=True)

    def finish(fig, name, title, subtitle):
        fig.suptitle(title + '\n' + subtitle, fontsize=14, x=.5)
        footer = ('PG18.3 · candidate upstream SQLx socket fix · control-plane probe' if name == 'cron-protocol'
                  else 'PG18.3 · identical upstream SQLx socket fix · sequential single-host comparison')
        fig.text(.01, .005, footer,
                 fontsize=9, color='#555555')
        fig.tight_layout(rect=(0, .025, 1, .98))
        for extension in ('png', 'svg'):
            fig.savefig(out / f'{name}.{extension}', dpi=180, metadata={'Date': None} if extension == 'svg' else None)
        plt.close(fig)

    workloads = ['ref800', 'sat-w64', 'sat-w128', 'sat-w256']
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for ax, key, title, unit in zip(axes,
            ['median_throughput_per_s', 'median_end_to_end_p99_ms'],
            ['Handler completion rate', 'Median rolling-window p99'], ['Jobs / second', 'Milliseconds']):
        for offset, build in zip([-.19, .19], COLORS):
            values = [phases[f'{w}-{build}']['clean'][key] for w in workloads]
            bars = ax.bar(np.arange(4) + offset, values, width=.36, color=COLORS[build], label=labels[build])
            ax.bar_label(bars, labels=[f'{v:,.0f}' for v in values], fontsize=9, padding=3)
        ax.set_xticks(range(4), ['W32\n800/s', 'W64\nSaturation', 'W128\nSaturation', 'W256\nSaturation'])
        ax.set_title(title); ax.set_ylabel(unit); ax.set_ylim(0, ax.get_ylim()[1] * 1.16)
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        ax.grid(axis='y', alpha=.18); ax.set_axisbelow(True)
    axes[0].legend(frameon=False, fontsize=9)
    finish(fig, 'throughput-latency', 'AWA · reference and saturation',
           'One pair per workload; bars are phase medians, not uncertainty intervals')

    series = {}
    for build in COLORS:
        cell = f'mvcc-soak-{build}'
        if cell not in phases:
            continue
        name = 'soak-series.json' if build == 'candidate' else 'soak-baseline-series.json'
        if not (root / name).exists():
            from plot_awa_soak import plot
            plot(root, cell)
        series[build] = json.loads((root / name).read_text())
    if series:
        metrics = [('completion_rate', 'Complete / second'), ('end_to_end_p99_ms', 'Window p99 (ms)'),
                   ('queue_depth', 'Queued jobs'), ('n_dead_tup', 'Dead tuples'),
                   ('total_relation_size_mb', 'Relations (MiB)')]
        fig, axes = plt.subplots(5, 1, figsize=(13, 12), sharex=True)
        # Phase boundaries are checked for comparability instead of silently aligning unlike runs.
        bounds = [v['phase_start_s'] for v in series.values()]
        for phase in ('clean', 'pinned', 'recovery'):
            if max(b[phase] for b in bounds) - min(b[phase] for b in bounds) > 5:
                raise ValueError('Paired soak phase boundaries differ by more than one sample')
        pin, recovery = bounds[0]['pinned'] / 60, bounds[0]['recovery'] / 60
        for ax, (metric, ylabel) in zip(axes, metrics):
            for build, data in series.items():
                xy = data['series'][metric]
                ax.plot([t / 60 for t, _ in xy], [v for _, v in xy], color=COLORS[build],
                        linestyle=STYLES[build], linewidth=.85, alpha=.85, label=labels[build])
            ax.axvspan(pin, recovery, color='#777777', alpha=.09)
            ax.axvline(recovery, color='#555555', linewidth=.8)
            ax.set_ylabel(ylabel); ax.grid(alpha=.16); ax.set_ylim(bottom=0)
        axes[0].axhline(800, color='#555555', linestyle=':', linewidth=.8)
        axes[0].legend(frameon=False, loc='lower left')
        axes[0].text((pin + recovery) / 2, 1.02, f'{recovery-pin:.0f}-minute MVCC pin',
                     transform=axes[0].get_xaxis_transform(), ha='center', fontsize=10)
        axes[-1].set_xlabel('Elapsed minutes in each separate soak (warmup included)')
        finish(fig, 'soak-comparison', 'AWA · MVCC soak comparison',
               'W32 · offered 800/s · every five-second sample · shared axes for both builds')

        fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
        for ax, minutes, title in zip(axes, [5, 80], ['First five minutes', 'Full recovery window']):
            for build, data in series.items():
                start = data['phase_start_s']['recovery']
                xy = [(t-start, v) for t, v in data['series']['n_dead_tup'] if start <= t <= start + minutes * 60]
                ax.plot([t/60 for t,v in xy], [v for t,v in xy], color=COLORS[build],
                        linestyle=STYLES[build], linewidth=1, label=labels[build])
                clean = phases[f'mvcc-soak-{build}']['clean']['median_dead_tup'] * 1.1
                ax.axhline(clean, color=COLORS[build], linestyle=':', linewidth=1,
                           label=f'{build.title()} clean threshold ({clean:.1f})')
            ax.set_title(title); ax.set_xlabel('Minutes from first recovery sample')
            ax.set_xlim(0, minutes); ax.set_ylim(bottom=0); ax.grid(alpha=.16)
        axes[0].set_ylabel('Estimated dead tuples in tracked tables')
        axes[1].legend(frameon=False, fontsize=8)
        finish(fig, 'recovery', 'AWA · dead-tuple recovery under continued traffic',
               'Threshold crossings are first observations; they do not imply sustained return to baseline')

    rows = [json.loads(line) for line in (root / 'cron-protocol.jsonl').read_text().splitlines()]
    fleets = sorted({r['fleet'] for r in rows}); schedules = sorted({r['schedules'] for r in rows})
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for ax, key, title in zip(axes, ['steady_publication', 'reconcile_ms'],
                             ['Steady publication p99 (ms)', 'Reconciliation elapsed (ms)']):
        matrix = np.array([[next(r[key]['p99_ms'] if key == 'steady_publication' else r[key]
                                for r in rows if r['fleet'] == f and r['schedules'] == s)
                            for s in schedules] for f in fleets])
        mesh = ax.imshow(matrix, cmap='Blues', vmin=0, aspect='auto')
        for i in range(len(fleets)):
            for j in range(len(schedules)):
                ax.text(j, i, f'{matrix[i,j]:.1f}', ha='center', va='center',
                        color='white' if matrix[i,j] > matrix.max()*.6 else '#222222')
        ax.set_xticks(range(len(schedules)), [f'{s:,}' for s in schedules])
        ax.set_yticks(range(len(fleets)), fleets); ax.set_xlabel('Schedules'); ax.set_ylabel('Runtimes')
        ax.set_title(title); fig.colorbar(mesh, ax=ax, shrink=.8, label='ms')
    finish(fig, 'cron-protocol', 'AWA · candidate cron control plane',
           'Prepared manifests · three steady publication rounds · scales are specific to each metric')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    render(parser.parse_args().campaign)
