"""
Rawft vs GoRaft Benchmark Visualization
Payload=100B, Batch=16, Threads=[1,2,4,8,16,32,64,128]
Python 3.6+ compatible
"""

import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stat_image")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Raw data (Payload=100B, Batch=16, Threads=[1,2,4,8,16,32,64,128])
threads = [1, 2, 4, 8, 16, 32, 64, 128]

# =========================
# Rawft (NVMe-oF)
# =========================
nvme_throughput_raw = [
    [5027.16],
    [8550.84],
    [13535.64],
    [22731.83],
    [28152.39],
    [34685.55],
    [42528.63],
    [55346.08],
]

nvme_latency_avg_raw = [  # µs
    [192.668],
    [227.34],
    [288.65],
    [343.945],
    [556.467],
    [904.016],
    [1480.299],
    [2281.844],
]

nvme_latency_p99_raw = [  # µs
    [262.621],
    [347.908],
    [475.498],
    [926.918],
    [1409.98],
    [4427.19],
    [5518.405],
    [4446.571],
]

# =========================
# GoRaft (baseline)
# =========================
goraft_throughput_raw = [
    [1276.22],
    [1488.53],
    [1665.76],
    [1672.52],
    [1608.61],
    [1502.36],
    [1495.56],
    [1527.36],
]

goraft_latency_avg_raw = [  # µs
    [777.556],
    [1336.923],
    [2394.377],
    [4771.867],
    [9883.931],
    [21188.141],
    [42601.14],
    [83188.875],
]

goraft_latency_p99_raw = [  # µs
    [929.62],
    [1640.895],
    [2887.562],
    [6392.067],
    [13971.547],
    [27018.739],
    [47427.472],
    [120558.753],
]


def mean(lst):
    return sum(lst) / len(lst)

def std(lst):
    m = mean(lst)
    return (sum((x - m) ** 2 for x in lst) / len(lst)) ** 0.5


nvme_tp    = [mean(r) for r in nvme_throughput_raw]
nvme_tp_e  = [std(r)  for r in nvme_throughput_raw]
nvme_avg   = [mean(r) for r in nvme_latency_avg_raw]
nvme_avg_e = [std(r)  for r in nvme_latency_avg_raw]
nvme_p99   = [mean(r) for r in nvme_latency_p99_raw]
nvme_p99_e = [std(r)  for r in nvme_latency_p99_raw]

goraft_tp    = [mean(r) for r in goraft_throughput_raw]
goraft_tp_e  = [std(r)  for r in goraft_throughput_raw]
goraft_avg   = [mean(r) for r in goraft_latency_avg_raw]
goraft_avg_e = [std(r)  for r in goraft_latency_avg_raw]
goraft_p99   = [mean(r) for r in goraft_latency_p99_raw]
goraft_p99_e = [std(r)  for r in goraft_latency_p99_raw]


C_NVME     = '#2563EB'
C_GORAFT   = '#DC2626'
C_NVME_L   = '#93C5FD'
C_GORAFT_L = '#FCA5A5'

FIGSIZE        = (7, 5)
FONTSIZE_TITLE = 13
FONTSIZE_LABEL = 11
FONTSIZE_TICK  = 10
PAYLOAD_LABEL  = 'Batch=16, Payload=100B'


# Figure 1: Throughput vs Threads
fig, ax = plt.subplots(figsize=FIGSIZE)
x = np.arange(len(threads))
width = 0.35

bars1 = ax.bar(x - width/2, nvme_tp, width, color=C_NVME, label='Rawft (PBA)')
bars2 = ax.bar(x + width/2, goraft_tp, width, color=C_GORAFT, label='goraft (baseline)')

for i in range(len(threads)):
    ratio = nvme_tp[i] / goraft_tp[i]
    ax.annotate('{:.1f}x'.format(ratio), xy=(x[i] - width/2, nvme_tp[i]),
                xytext=(0, 5), textcoords='offset points',
                ha='center', fontsize=8, color=C_NVME)

ax.set_xticks(x)
ax.set_xticklabels(threads)

ax.set_xlabel('Number of Client Threads', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Throughput (entries/s)', fontsize=FONTSIZE_LABEL)
ax.set_title('Throughput vs Threads\n({})'.format(PAYLOAD_LABEL), fontsize=FONTSIZE_TITLE)

ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
ax.set_ylim(0, max(nvme_tp) * 1.25)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '1_throughput_vs_threads_100B.png'), dpi=150)
plt.close()
print('Saved: 1_throughput_vs_threads_100B.png')


# Figure 2: Avg Latency vs Threads
x = np.arange(len(threads))

fig, ax = plt.subplots(figsize=FIGSIZE)
ax.errorbar(x, nvme_avg, yerr=nvme_avg_e,
            color=C_NVME, marker='o', linewidth=2, capsize=4,
            label='Rawft  (PBA)')
ax.errorbar(x, goraft_avg, yerr=goraft_avg_e,
            color=C_GORAFT, marker='s', linewidth=2, capsize=4,
            label='goraft (baseline)')
for i in range(len(threads)):
    ratio = goraft_avg[i] / nvme_avg[i]
    ax.annotate('{:.1f}x'.format(ratio), xy=(x[i], goraft_avg[i]),
                xytext=(0, 8), textcoords='offset points',
                ha='center', fontsize=8, color=C_GORAFT)

ax.set_xticks(x)
ax.set_xticklabels(threads)
ax.set_xlabel('Number of Client Threads', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Avg Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('Avg Latency vs Threads\n({})'.format(PAYLOAD_LABEL), fontsize=FONTSIZE_TITLE)

ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '2_latency_avg_vs_threads_100B.png'), dpi=150)
plt.close()
print('Saved: 2_latency_avg_vs_threads_100B.png')


# Figure 3: p99 Latency vs Threads
fx = np.arange(len(threads))

fig, ax = plt.subplots(figsize=FIGSIZE)
ax.errorbar(x, nvme_avg, yerr=nvme_avg_e,
            color=C_NVME, marker='o', linewidth=2, capsize=4,
            label='Rawft  (PBA)')
ax.errorbar(x, goraft_avg, yerr=goraft_avg_e,
            color=C_GORAFT, marker='s', linewidth=2, capsize=4,
            label='goraft (baseline)')
for i in range(len(threads)):
    ratio = goraft_avg[i] / nvme_avg[i]
    ax.annotate('{:.1f}x'.format(ratio), xy=(x[i], goraft_avg[i]),
                xytext=(0, 8), textcoords='offset points',
                ha='center', fontsize=8, color=C_GORAFT)

ax.set_xticks(x)
ax.set_xticklabels(threads)
ax.set_xlabel('Number of Client Threads', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('p99 Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('p99 Latency vs Threads\n({})'.format(PAYLOAD_LABEL), fontsize=FONTSIZE_TITLE)

ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '3_latency_p99_vs_threads_100B.png'), dpi=150)
plt.close()
print('Saved: 3_latency_p99_vs_threads_100B.png')


# Figure 4: Latency-Throughput Tradeoff (dual x-axis)
# Bottom x-axis: Rawft  scale
# Top    x-axis: goraft scale (independent via twiny)
# y-axis: avg latency (shared)
fig, ax_bot = plt.subplots(figsize=(8, 5))

ax_bot.errorbar(nvme_tp, nvme_avg,
                xerr=nvme_tp_e, yerr=nvme_avg_e,
                color=C_NVME, marker='o', linewidth=1.5,
                capsize=3, linestyle='-', label='Rawft  (PBA)')
for i, t in enumerate(threads):
    ax_bot.annotate('T={}'.format(t), xy=(nvme_tp[i], nvme_avg[i]),
                    xytext=(4, -10), textcoords='offset points',
                    fontsize=7, color=C_NVME)
ax_bot.set_xlabel('Throughput - Rawft  (entries/s)',
                  fontsize=FONTSIZE_LABEL, color=C_NVME)
ax_bot.set_ylabel('Avg Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax_bot.tick_params(axis='x', labelcolor=C_NVME, labelsize=FONTSIZE_TICK)
ax_bot.tick_params(axis='y', labelsize=FONTSIZE_TICK)
ax_bot.grid(True, linestyle='--', alpha=0.4)

ax_top = ax_bot.twiny()
ax_top.errorbar(goraft_tp, goraft_avg,
                xerr=goraft_tp_e, yerr=goraft_avg_e,
                color=C_GORAFT, marker='s', linewidth=1.5,
                capsize=3, linestyle='-', label='goraft (baseline)')
for i, t in enumerate(threads):
    ax_top.annotate('T={}'.format(t), xy=(goraft_tp[i], goraft_avg[i]),
                    xytext=(4, 6), textcoords='offset points',
                    fontsize=7, color=C_GORAFT)
ax_top.set_xlabel('Throughput - goraft (entries/s)',
                  fontsize=FONTSIZE_LABEL, color=C_GORAFT)
ax_top.tick_params(axis='x', labelcolor=C_GORAFT, labelsize=FONTSIZE_TICK)

h1, l1 = ax_bot.get_legend_handles_labels()
h2, l2 = ax_top.get_legend_handles_labels()
ax_bot.legend(h1 + h2, l1 + l2, fontsize=FONTSIZE_TICK, loc='upper right')

ax_bot.set_title('Latency-Throughput Tradeoff (Dual X-axis)\n({})'.format(PAYLOAD_LABEL),
                 fontsize=FONTSIZE_TITLE, y=1.12)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '4_latency_throughput_scatter_100B.png'), dpi=150)
plt.close()
print('Saved: 4_latency_throughput_scatter_100B.png')


# Figure 4b: Latency-Throughput — Rawft  only
fig, ax = plt.subplots(figsize=FIGSIZE)
ax.errorbar(nvme_tp, nvme_avg, xerr=nvme_tp_e, yerr=nvme_avg_e,
            color=C_NVME, marker='o', linewidth=2, capsize=4,
            linestyle='-', label='avg latency')
ax.errorbar(nvme_tp, nvme_p99, xerr=nvme_tp_e, yerr=nvme_p99_e,
            color=C_NVME_L, marker='^', linewidth=2, capsize=4,
            linestyle='--', label='p99 latency')
for i, t in enumerate(threads):
    ax.annotate('T={}'.format(t), xy=(nvme_tp[i], nvme_avg[i]),
                xytext=(5, -10), textcoords='offset points',
                fontsize=8, color='#1e40af')
ax.set_xlabel('Throughput (entries/s)', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('Rawft (PBA) - Latency-Throughput Tradeoff\n({})'.format(PAYLOAD_LABEL),
             fontsize=FONTSIZE_TITLE)
ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
ax.annotate('better', xy=(0.97, 0.08), xycoords='axes fraction',
            ha='right', va='bottom', fontsize=9, color='gray')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '4b_latency_throughput_nvmeof_100B.png'), dpi=150)
plt.close()
print('Saved: 4b_latency_throughput_nvmeof_100B.png')


# Figure 4c: Latency-Throughput — goraft only
fig, ax = plt.subplots(figsize=FIGSIZE)
ax.errorbar(goraft_tp, goraft_avg, xerr=goraft_tp_e, yerr=goraft_avg_e,
            color=C_GORAFT, marker='s', linewidth=2, capsize=4,
            linestyle='-', label='avg latency')
ax.errorbar(goraft_tp, goraft_p99, xerr=goraft_tp_e, yerr=goraft_p99_e,
            color=C_GORAFT_L, marker='^', linewidth=2, capsize=4,
            linestyle='--', label='p99 latency')
for i, t in enumerate(threads):
    ax.annotate('T={}'.format(t), xy=(goraft_tp[i], goraft_avg[i]),
                xytext=(5, -10), textcoords='offset points',
                fontsize=8, color='#991b1b')
ax.set_xlabel('Throughput (entries/s)', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('goraft - Latency-Throughput Tradeoff\n({})'.format(PAYLOAD_LABEL),
             fontsize=FONTSIZE_TITLE)
ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
ax.annotate('better', xy=(0.97, 0.08), xycoords='axes fraction',
            ha='right', va='bottom', fontsize=9, color='gray')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '4c_latency_throughput_goraft_100B.png'), dpi=150)
plt.close()
print('Saved: 4c_latency_throughput_goraft_100B.png')


# Figure 5: Bar chart — peak throughput thread comparison
peak_nvme_idx   = nvme_tp.index(max(nvme_tp))      # T=8
peak_goraft_idx = goraft_tp.index(max(goraft_tp))  # T=2

fig, axes = plt.subplots(1, 3, figsize=(11, 5))
labels = ['Rawft \n(PBA)', 'goraft\n(baseline)']
colors = [C_NVME, C_GORAFT]

vals_tp  = [nvme_tp[peak_nvme_idx],   goraft_tp[peak_goraft_idx]]
errs_tp  = [nvme_tp_e[peak_nvme_idx], goraft_tp_e[peak_goraft_idx]]
vals_avg = [nvme_avg[peak_nvme_idx],   goraft_avg[peak_goraft_idx]]
errs_avg = [nvme_avg_e[peak_nvme_idx], goraft_avg_e[peak_goraft_idx]]
vals_p99 = [nvme_p99[peak_nvme_idx],   goraft_p99[peak_goraft_idx]]
errs_p99 = [nvme_p99_e[peak_nvme_idx], goraft_p99_e[peak_goraft_idx]]

axes[0].bar(labels, vals_tp, yerr=errs_tp, color=colors, capsize=5, width=0.5)
axes[0].set_title('Throughput\n(peak T each)', fontsize=FONTSIZE_LABEL, y=1.02)
axes[0].set_ylabel('entries/s', fontsize=FONTSIZE_TICK)
axes[0].tick_params(labelsize=FONTSIZE_TICK)
axes[0].grid(True, axis='y', linestyle='--', alpha=0.5)
axes[0].text(0.5, max(vals_tp) * 1.05,
             '{:.1f}x faster'.format(vals_tp[0] / vals_tp[1]),
             ha='center', fontsize=10, fontweight='bold')

axes[1].bar(labels, vals_avg, yerr=errs_avg, color=colors, capsize=5, width=0.5)
axes[1].set_title('Avg Latency/Batch\n(peak T each)', fontsize=FONTSIZE_LABEL, y=1.02)
axes[1].set_ylabel('us', fontsize=FONTSIZE_TICK)
axes[1].tick_params(labelsize=FONTSIZE_TICK)
axes[1].grid(True, axis='y', linestyle='--', alpha=0.5)
axes[1].text(0.5, max(vals_avg) * 1.05,
             '{:.1f}x lower'.format(vals_avg[1] / vals_avg[0]),
             ha='center', fontsize=10, fontweight='bold')

axes[2].bar(labels, vals_p99, yerr=errs_p99, color=colors, capsize=5, width=0.5)
axes[2].set_title('p99 Latency/Batch\n(peak T each)', fontsize=FONTSIZE_LABEL, y=1.02)
axes[2].set_ylabel('us', fontsize=FONTSIZE_TICK)
axes[2].tick_params(labelsize=FONTSIZE_TICK)
axes[2].grid(True, axis='y', linestyle='--', alpha=0.5)
axes[2].text(0.5, max(vals_p99) * 1.05,
             '{:.1f}x lower'.format(vals_p99[1] / vals_p99[0]),
             ha='center', fontsize=10, fontweight='bold')

fig.suptitle('Rawft  vs goraft  |  {}  (at peak throughput threads)'.format(PAYLOAD_LABEL),
             fontsize=FONTSIZE_TITLE, y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '5_bar_comparison_peak_100B.png'),
            dpi=150, bbox_inches='tight')
plt.close()
print('Saved: 5_bar_comparison_peak_100B.png')


# Figure 6: Combined Avg + p99 Latency — Rawft vs goraft
fig, ax = plt.subplots(figsize=(8, 5))
x = np.arange(len(threads))

ax.plot(x, nvme_avg, color=C_NVME, marker='o', linewidth=2,
        linestyle='-', label='Rawft avg')
ax.plot(x, nvme_p99, color=C_NVME_L, marker='^', linewidth=2,
        linestyle='--', label='Rawft p99')
ax.plot(x, goraft_avg, color=C_GORAFT, marker='s', linewidth=2,
        linestyle='-', label='goraft avg')
ax.plot(x, goraft_p99, color=C_GORAFT_L, marker='D', linewidth=2,
        linestyle='--', label='goraft p99')


ax.set_xticks(x)
ax.set_xticklabels(threads)
ax.set_xlabel('Number of Client Threads', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('Avg & p99 Latency vs Threads\n({})'.format(PAYLOAD_LABEL),
             fontsize=FONTSIZE_TITLE)
ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK)
ax.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, '6_latency_combined_100B.png'), dpi=150)
plt.close()
print('Saved: 6_latency_combined_100B.png')

print('\nAll graphs saved to:', OUTPUT_DIR)