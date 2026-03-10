"""Generate the architecture diagram (plots/architecture.png)."""

import os

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

# GitHub Dark theme
BG = "#0D1117"
TEXT = "#E6EDF3"

# Accent colours (from plot.py palette)
YELLOW = "#D29922"
BLUE = "#58A6FF"
GREEN = "#3FB950"
TEAL = "#39D2C0"
PINK = "#DB61A2"
PURPLE = "#BC8CFF"

EXCHANGES = ["Binance", "Coinbase", "OKX", "Bybit", "Upbit", "Gate", "Kucoin", "MEXC"]

# Layout constants
CX = 50
BW, BH = 32, 9
BOX_PAD = 0.4
BAR_W, BAR_H = 86, 8
BAR_PAD = 0.5


def _box(ax, cy, color, title, subtitle):
    """Rounded box with centred title + subtitle."""
    patch = mpatches.FancyBboxPatch(
        (CX - BW / 2, cy - BH / 2), BW, BH,
        boxstyle=f"round,pad={BOX_PAD}",
        facecolor=BG, edgecolor=color, linewidth=2.2,
    )
    ax.add_patch(patch)
    ax.text(CX, cy + BH * 0.13, title,
            ha="center", va="center",
            fontsize=15, fontweight="bold", fontfamily="monospace", color=TEXT)
    ax.text(CX, cy - BH * 0.25, subtitle,
            ha="center", va="center", fontsize=10, color=color)


def _arrow(ax, y_from, y_to, color):
    """Vertical arrow between two y positions."""
    ax.annotate(
        "", xy=(CX, y_to), xytext=(CX, y_from),
        arrowprops=dict(arrowstyle="-|>", color=color, lw=1.8, mutation_scale=15),
    )


def main():
    fig, ax = plt.subplots(figsize=(13, 10))
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # y-centres (top to bottom)
    Y_CFG, Y_CONN, Y_BAR, Y_SQL, Y_BT, Y_PLT = 90, 73, 56, 40, 25, 11

    # boxes
    _box(ax, Y_CFG,  YELLOW, "config.yaml",  "tickers: [BTC, PENGU]")
    _box(ax, Y_CONN, BLUE,   "connector.py", "async orchestrator · Queue(50k)")
    _box(ax, Y_SQL,  TEAL,   "SQLite (WAL)", "order_book · index(ticker, ts)")
    _box(ax, Y_BT,   PINK,   "backtest.py",  "replay · spread · cooldown · profit")
    _box(ax, Y_PLT,  BLUE,   "plot.py",      "6 matplotlib charts")

    # exchange bar
    bar = mpatches.FancyBboxPatch(
        (CX - BAR_W / 2, Y_BAR - BAR_H / 2), BAR_W, BAR_H,
        boxstyle=f"round,pad={BAR_PAD}",
        facecolor="#162B16", edgecolor=GREEN, linewidth=1.8, alpha=0.85,
    )
    ax.add_patch(bar)
    cell_w = BAR_W / len(EXCHANGES)
    x0 = CX - BAR_W / 2
    for i, name in enumerate(EXCHANGES):
        if i:
            x_sep = x0 + i * cell_w
            ax.plot([x_sep, x_sep],
                    [Y_BAR - BAR_H / 2 + 0.6, Y_BAR + BAR_H / 2 - 0.6],
                    color=GREEN, lw=0.7, alpha=0.6)
        ax.text(x0 + (i + 0.5) * cell_w, Y_BAR, name,
                ha="center", va="center", fontsize=11, fontweight="bold",
                color=GREEN)

    # arrows (bottom of source to top of target)
    gap = 0.3
    _arrow(ax, Y_CFG  - BH / 2 - BOX_PAD - gap, Y_CONN + BH / 2 + BOX_PAD + gap, YELLOW)
    _arrow(ax, Y_CONN - BH / 2 - BOX_PAD - gap, Y_BAR  + BAR_H / 2 + BAR_PAD + gap, BLUE)
    _arrow(ax, Y_BAR  - BAR_H / 2 - BAR_PAD - gap, Y_SQL + BH / 2 + BOX_PAD + gap, TEAL)
    _arrow(ax, Y_SQL  - BH / 2 - BOX_PAD - gap, Y_BT   + BH / 2 + BOX_PAD + gap, TEAL)
    _arrow(ax, Y_BT   - BH / 2 - BOX_PAD - gap, Y_PLT  + BH / 2 + BOX_PAD + gap, PURPLE)

    os.makedirs("plots", exist_ok=True)
    fig.savefig("plots/architecture.png", dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print("  plots/architecture.png")


if __name__ == "__main__":
    main()
