"""Backtest result visualisation. Generates PNGs in plots/."""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.axes import Axes
from matplotlib.figure import Figure
import numpy as np

# --- GitHub Dark theme colours ---
BG: str = "#0D1117"
GRID: str = "#21262D"
TEXT: str = "#E6EDF3"
ACCENT_COLOURS: list[str] = [
    "#58A6FF",  # blue
    "#3FB950",  # green
    "#D29922",  # yellow
    "#F85149",  # red
    "#BC8CFF",  # purple
    "#39D2C0",  # teal
    "#DB61A2",  # pink
    "#F0883E",  # orange
]


type Opportunity = dict[str, str | int | float]


def apply_style(ax: Axes) -> None:
    """Apply GitHub Dark theme colours to a matplotlib axes."""
    ax.set_facecolor(BG)
    ax.figure.set_facecolor(BG)
    ax.tick_params(colors=TEXT, labelsize=9)
    ax.xaxis.label.set_color(TEXT)
    ax.yaxis.label.set_color(TEXT)
    ax.title.set_color(TEXT)
    ax.grid(True, color=GRID, linewidth=0.5)
    for spine in ax.spines.values():
        spine.set_color(GRID)


def save_fig(fig: Figure, path: str) -> None:
    """Save a figure to disk as PNG and close it."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  {path}")


def load_data(path: str = "backtest.json") -> list[Opportunity]:
    """Load backtest results from a JSON file."""
    with open(path) as f:
        return json.load(f)


# ── 1. Cumulative profit over time ──────────────────────────────────

def plot_cumulative_profit(opportunities: list[Opportunity]) -> None:
    """Plot cumulative profit over time as a filled line chart."""
    sorted_opportunities = sorted(opportunities, key=lambda opportunity: opportunity["start_ts"])
    timestamps = [datetime.fromtimestamp(opportunity["start_ts"] / 1000, tz=timezone.utc)
                  for opportunity in sorted_opportunities]
    cumulative_profit = []
    total = 0.0
    for opportunity in sorted_opportunities:
        total += opportunity["profit_usdt"]
        cumulative_profit.append(total)

    fig, ax = plt.subplots(figsize=(10, 4))
    apply_style(ax)
    ax.plot(timestamps, cumulative_profit, color=ACCENT_COLOURS[0], linewidth=1.5)
    ax.fill_between(timestamps, cumulative_profit, alpha=0.15, color=ACCENT_COLOURS[0])
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Cumulative Profit (USDT)")
    ax.set_title("Cumulative Profit Over Time")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f"))
    fig.autofmt_xdate()
    save_fig(fig, "plots/cumulative_profit.png")


# ── 2. Spread distribution ─────────────────────────────────────────

def plot_spread_distribution(opportunities: list[Opportunity]) -> None:
    """Plot a histogram of mean spread percentages."""
    spreads = [opportunity["mean_pct"] for opportunity in opportunities]

    fig, ax = plt.subplots(figsize=(8, 4))
    apply_style(ax)
    ax.hist(spreads, bins=30, color=ACCENT_COLOURS[1], edgecolor=GRID, alpha=0.85)
    ax.set_xlabel("Mean Spread (%)")
    ax.set_ylabel("Count")
    ax.set_title("Distribution of Captured Spreads")
    save_fig(fig, "plots/spread_distribution.png")


# ── 3. Profit vs Duration scatter ──────────────────────────────────

def plot_profit_vs_duration(opportunities: list[Opportunity]) -> None:
    """Scatter plot of profit versus opportunity duration on a log scale."""
    durations = np.array([opportunity["duration_ms"] / 1000 for opportunity in opportunities])
    profits = np.array([opportunity["profit_usdt"] for opportunity in opportunities])

    fig, ax = plt.subplots(figsize=(10, 5))
    apply_style(ax)

    ax.scatter(durations, profits, color=ACCENT_COLOURS[0], alpha=0.6, s=35,
               edgecolors="none")

    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda value, _: f"{value:.0f}s" if value >= 1 else f"{value*1000:.0f}ms"))
    ax.set_xlabel("Duration (log scale)")
    ax.set_ylabel("Profit (USDT)")
    ax.set_title("Profit vs Duration")

    median_duration = float(np.median(durations))
    ax.axvline(median_duration, color=TEXT, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(median_duration * 1.15, profits.max() * 0.95, f"median {median_duration:.1f}s",
            color=TEXT, fontsize=8, alpha=0.7)

    save_fig(fig, "plots/profit_vs_duration.png")


# ── 4. Profit heatmap (ask_exchange × bid_exchange) ─────────────────

def plot_profit_heatmap(opportunities: list[Opportunity]) -> None:
    """Plot a heatmap of total profit for each (ask exchange, bid exchange) pair."""
    profit_by_pair = defaultdict(lambda: defaultdict(float))
    exchanges = set()
    for opportunity in opportunities:
        profit_by_pair[opportunity["ask_exchange"]][opportunity["bid_exchange"]] += opportunity["profit_usdt"]
        exchanges.add(opportunity["ask_exchange"])
        exchanges.add(opportunity["bid_exchange"])

    exchanges = sorted(exchanges)
    matrix = np.zeros((len(exchanges), len(exchanges)))
    for row_index, ask in enumerate(exchanges):
        for col_index, bid in enumerate(exchanges):
            matrix[row_index][col_index] = profit_by_pair[ask][bid]

    fig, ax = plt.subplots(figsize=(7, 6))
    apply_style(ax)
    heatmap_image = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
    ax.set_xticks(range(len(exchanges)))
    ax.set_yticks(range(len(exchanges)))
    ax.set_xticklabels(exchanges, rotation=45, ha="right")
    ax.set_yticklabels(exchanges)
    ax.set_xlabel("Bid Exchange (sell)")
    ax.set_ylabel("Ask Exchange (buy)")
    ax.set_title("Profit Heatmap (USDT)")

    for row_index in range(len(exchanges)):
        for col_index in range(len(exchanges)):
            cell_value = matrix[row_index][col_index]
            if cell_value > 0:
                color = "#0D1117" if cell_value > matrix.max() * 0.6 else TEXT
                ax.text(col_index, row_index, f"{cell_value:.2f}", ha="center", va="center",
                        fontsize=8, color=color)

    colorbar = fig.colorbar(heatmap_image, ax=ax, shrink=0.8)
    colorbar.ax.yaxis.set_tick_params(color=TEXT)
    colorbar.outline.set_edgecolor(GRID)
    plt.setp(colorbar.ax.yaxis.get_ticklabels(), color=TEXT, fontsize=8)
    save_fig(fig, "plots/profit_heatmap.png")


# ── 5. Opportunities per hour ──────────────────────────────────────

def plot_opportunities_per_hour(opportunities: list[Opportunity]) -> None:
    """Bar chart of opportunity count per hour with a profit overlay line."""
    hours = [datetime.fromtimestamp(opportunity["start_ts"] / 1000, tz=timezone.utc).hour
             for opportunity in opportunities]
    counts = [0] * 24
    profits = [0.0] * 24
    for opportunity, hour in zip(opportunities, hours):
        counts[hour] += 1
        profits[hour] += opportunity["profit_usdt"]

    fig, ax1 = plt.subplots(figsize=(10, 4))
    apply_style(ax1)

    hour_range = range(24)
    ax1.bar(hour_range, counts, color=ACCENT_COLOURS[0], alpha=0.7, label="Opportunities")
    ax1.set_xlabel("Hour (UTC)")
    ax1.set_ylabel("# Opportunities")
    ax1.set_title("Activity by Hour of Day")
    ax1.set_xticks(hour_range)
    ax1.set_xticklabels([f"{hour:02d}" for hour in hour_range], fontsize=8)

    ax2 = ax1.twinx()
    ax2.plot(hour_range, profits, color=ACCENT_COLOURS[3], marker="o", markersize=4,
             linewidth=1.5, label="Profit")
    ax2.set_ylabel("Profit (USDT)")
    ax2.yaxis.label.set_color(TEXT)
    ax2.tick_params(colors=TEXT, labelsize=9)
    for spine in ax2.spines.values():
        spine.set_color(GRID)

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, fontsize=8, facecolor=BG, edgecolor=GRID,
               labelcolor=TEXT, loc="upper left")
    save_fig(fig, "plots/opps_per_hour.png")


# ── 6. Spread vs Profit ───────────────────────────────────────────

def plot_spread_vs_profit(opportunities: list[Opportunity]) -> None:
    """Bubble chart of spread versus profit, with bubble size proportional to quantity."""
    fig, ax = plt.subplots(figsize=(8, 5))
    apply_style(ax)

    spreads = [opportunity["mean_pct"] for opportunity in opportunities]
    profits = [opportunity["profit_usdt"] for opportunity in opportunities]
    quantities = [opportunity["qty"] for opportunity in opportunities]

    sizes = np.array(quantities)
    sizes = 15 + 80 * (sizes - sizes.min()) / (sizes.max() - sizes.min() + 1e-9)

    ax.scatter(spreads, profits, s=sizes, color=ACCENT_COLOURS[4], alpha=0.6,
               edgecolors=GRID, linewidths=0.3)
    ax.set_xlabel("Mean Spread (%)")
    ax.set_ylabel("Profit (USDT)")
    ax.set_title("Spread vs Profit (size = quantity)")
    save_fig(fig, "plots/spread_vs_profit.png")


# ── Main ────────────────────────────────────────────────────────────

def main() -> None:
    """Load backtest results and generate all plots."""
    opportunities = load_data()
    print(f"Loaded {len(opportunities)} opportunities")
    if not opportunities:
        print("Nothing to plot.")
        return
    plot_cumulative_profit(opportunities)
    plot_spread_distribution(opportunities)
    plot_profit_vs_duration(opportunities)
    plot_profit_heatmap(opportunities)
    plot_opportunities_per_hour(opportunities)
    plot_spread_vs_profit(opportunities)
    print("Done.")


if __name__ == "__main__":
    main()
