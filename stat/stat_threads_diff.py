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

threads = [1, 2, 4, 8, 16, 32, 64, 128]

# ── Payload = 1024B ───────────────────────────────────────────────────────
nvme1024_tp  = [3509.24, 5618.75, 8914.66, 12708.88, 19670.86, 24931.45, 35083.93, 32309.41]
nvme1024_avg = [226.253, 300.362, 389.37,  560.385,  738.936,  1192.95,  1727.992, 3855.002]

goraft1024_tp  = [1027.72, 1167.68, 1327.82, 1383.45, 1469.78, 1468.10, 1556.90, 1572.12]
goraft1024_avg = [909.744, 1650.899, 2950.216, 5717.465, 10808.232, 21689.75, 40904.71, 80609.466]

# ── Payload = 100B ────────────────────────────────────────────────────────
nvme100_tp  = [5027.16, 8550.84, 13535.64, 22731.83, 28152.39, 34685.55, 42528.63, 55346.08]
nvme100_avg = [192.668, 227.34,  288.65,   343.945,  556.467,  904.016,  1480.299, 2281.844]

goraft100_tp  = [1276.22, 1488.53, 1665.76, 1672.52, 1608.61, 1502.36, 1495.56, 1527.36]
goraft100_avg = [777.556, 1336.923, 2394.377, 4771.867, 9883.931, 21188.141, 42601.14, 83188.875]

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
           title='nvmeof_raft (PBA)\nLatency-Throughput Tradeoff (Batch=16)')
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
           title='goraft (TCP)\nLatency-Throughput Tradeoff (Batch=16)')
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
ax.set_title('Latency-Throughput Tradeoff\nnvmeof_raft vs goraft · Payload 100B vs 1024B (Batch=16)',
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