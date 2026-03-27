"""
Latency-Throughput Tradeoff: Payload 100B vs 1024B
nvmeof_raft (PBA) vs goraft (TCP)
Two separate figures, each with two lines (payload 100B / 1024B)
Python 3.6+ compatible
"""

import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stat_image")
os.makedirs(OUTPUT_DIR, exist_ok=True)

threads = [1, 2, 4, 8, 16, 32, 64]

# ── Payload = 100B ────────────────────────────────────────────────────────
nvme1024_tp  = [7306.61, 11988.29, 18127.34, 26652.39, 29353.97, 29305.26, 23119.78]
nvme1024_avg = [107.814, 137.912,  190.068,  265.863,  508.194,  1044.155, 2717.260]

goraft1024_tp  = [1396.85, 1457.03, 1564.83, 1701.93, 1701.00, 1665.73, 1548.76]
goraft1024_avg = [687.458, 1343.794, 2524.819, 4665.786, 9362.775, 19122.815, 41023.043]

# ── Payload = 1024B ───────────────────────────────────────────────────────
nvme100_tp  = [10855.07, 18569.86, 29286.72, 38785.68, 44858.42, 49642.38, 58920.13]
nvme100_avg = [87.260,   102.970,  131.944,  201.924,  350.059,  580.041,  1076.414]

goraft100_tp  = [1285.84, 1305.83, 1317.59, 1324.58, 1339.64, 1307.70, 1298.49]
goraft100_avg = [771.579, 1527.161, 3035.942, 6047.531, 11939.704, 24511.734, 39164.679]

# ── Style ─────────────────────────────────────────────────────────────────
C_100B  = '#2563EB'   # blue  — payload 100B
C_1024B = '#16A34A'   # green — payload 1024B

FIGSIZE        = (7, 5)
FONTSIZE_TITLE = 13
FONTSIZE_LABEL = 11
FONTSIZE_TICK  = 10
FONTSIZE_ANNOT = 7


def annotate_threads(ax, tp, lat, color):
    """Annotate each point with its thread count."""
    for i, t in enumerate(threads):
        ax.annotate('T={}'.format(t),
                    xy=(tp[i], lat[i]),
                    xytext=(5, 4), textcoords='offset points',
                    fontsize=FONTSIZE_ANNOT, color=color)


def draw_graph(ax, tp100, lat100, tp1024, lat1024,
               title, xlabel='Throughput (entries/s)',
               ylabel='Avg Latency per Batch (us)'):
    ax.plot(tp100, lat100,
            color=C_100B, marker='o', linewidth=2, markersize=6,
            label='Payload 100B')
    annotate_threads(ax, tp100, lat100, C_100B)

    ax.plot(tp1024, lat1024,
            color=C_1024B, marker='s', linewidth=2, markersize=6,
            linestyle='--', label='Payload 1024B')
    annotate_threads(ax, tp1024, lat1024, C_1024B)

    ax.set_xlabel(xlabel, fontsize=FONTSIZE_LABEL)
    ax.set_ylabel(ylabel, fontsize=FONTSIZE_LABEL)
    ax.set_title(title, fontsize=FONTSIZE_TITLE)
    ax.tick_params(labelsize=FONTSIZE_TICK)
    ax.legend(fontsize=FONTSIZE_TICK, loc='upper right')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.annotate('better →', xy=(0.97, 0.05), xycoords='axes fraction',
                ha='right', va='bottom', fontsize=9, color='gray')


# ══════════════════════════════════════════════════════════════════════════
# Figure 1: nvmeof_raft (PBA) — 100B vs 1024B
# ══════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=FIGSIZE)
draw_graph(ax,
           tp100=nvme100_tp, lat100=nvme100_avg,
           tp1024=nvme1024_tp, lat1024=nvme1024_avg,
           title='nvmeof_raft (PBA)\nLatency-Throughput Tradeoff (Batch=32)')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'dual_payload_nvmeof.png'), dpi=150)
plt.close()
print('Saved: dual_payload_nvmeof.png')


# ══════════════════════════════════════════════════════════════════════════
# Figure 2: goraft (TCP) — 100B vs 1024B
# ══════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=FIGSIZE)
draw_graph(ax,
           tp100=goraft100_tp, lat100=goraft100_avg,
           tp1024=goraft1024_tp, lat1024=goraft1024_avg,
           title='goraft (TCP)\nLatency-Throughput Tradeoff (Batch=32)')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'dual_payload_goraft.png'), dpi=150)
plt.close()
print('Saved: dual_payload_goraft.png')

print('\nAll graphs saved to:', OUTPUT_DIR)
# ══════════════════════════════════════════════════════════════════════════
# Figure 3: All 4 lines combined
# ══════════════════════════════════════════════════════════════════════════
C_NVME_100   = '#2563EB'   # blue solid
C_NVME_1024  = '#16A34A'   # green solid
C_GORAFT_100  = '#DC2626'  # red solid
C_GORAFT_1024 = '#D97706'  # amber solid
 
fig, ax = plt.subplots(figsize=(8, 5))
 
ax.plot(nvme100_tp, nvme100_avg,
        color=C_NVME_100, marker='o', linewidth=2, markersize=6,
        linestyle='-', label='nvmeof_raft · 100B')
annotate_threads(ax, nvme100_tp, nvme100_avg, C_NVME_100)
 
ax.plot(nvme1024_tp, nvme1024_avg,
        color=C_NVME_1024, marker='s', linewidth=2, markersize=6,
        linestyle='-', label='nvmeof_raft · 1024B')
annotate_threads(ax, nvme1024_tp, nvme1024_avg, C_NVME_1024)
 
ax.plot(goraft100_tp, goraft100_avg,
        color=C_GORAFT_100, marker='^', linewidth=2, markersize=6,
        linestyle='--', label='goraft · 100B')
annotate_threads(ax, goraft100_tp, goraft100_avg, C_GORAFT_100)
 
ax.plot(goraft1024_tp, goraft1024_avg,
        color=C_GORAFT_1024, marker='D', linewidth=2, markersize=6,
        linestyle='--', label='goraft · 1024B')
annotate_threads(ax, goraft1024_tp, goraft1024_avg, C_GORAFT_1024)
 
ax.set_xlabel('Throughput (entries/s)', fontsize=FONTSIZE_LABEL)
ax.set_ylabel('Avg Latency per Batch (us)', fontsize=FONTSIZE_LABEL)
ax.set_title('Latency-Throughput Tradeoff\nnvmeof_raft vs goraft · Payload 100B vs 1024B (Batch=32)',
             fontsize=FONTSIZE_TITLE)
ax.tick_params(labelsize=FONTSIZE_TICK)
ax.legend(fontsize=FONTSIZE_TICK, loc='upper right')
ax.grid(True, linestyle='--', alpha=0.5)
ax.annotate('better →', xy=(0.97, 0.05), xycoords='axes fraction',
            ha='right', va='bottom', fontsize=9, color='gray')
 
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'dual_payload_combined.png'), dpi=150)
plt.close()
print('Saved: dual_payload_combined.png')