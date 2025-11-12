from pdb import run
import re
import pandas as pd
import argparse
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
import matplotlib.colors as mcolors
import colorsys
import os

def make_analogous_palette(n, base_hue_deg=200, spread_deg=40, s=0.50, l=0.62, pastel=0.25):
    """
    Analogous palette centered at base_hue_deg, spreading ±spread_deg/2.
    Returns n hex colors with slight pastel blend.
    """
    if n <= 1:
        hues = [base_hue_deg]
    else:
        start = base_hue_deg - spread_deg/2
        step = spread_deg / (n - 1)
        hues = [start + i*step for i in range(n)]

    colors = []
    for h in hues:
        r, g, b = colorsys.hls_to_rgb(h/360.0, l, s)
        # pastel blend toward white
        r = 1 - pastel*(1 - r)
        g = 1 - pastel*(1 - g)
        b = 1 - pastel*(1 - b)
        colors.append(mcolors.to_hex((r, g, b)))
    return colors

def lighten(hex_color, amount=0.25):
    rgb = mcolors.to_rgb(hex_color)
    return mcolors.to_hex(tuple(1 - amount*(1 - c) for c in rgb))

# Load the data
def load_data(file_path):
    df = pd.read_csv(file_path, header=0, skiprows=[1])
    return df

def split_data(big_df, column, n_parts, output_prefix,
               max_bar,
               bar_thinness, no_inner_spacing, group_width):
    """
    Split by unique queries into `n_parts` batches and plot each batch as a separate figure.
    Saves each figure to f"{output_prefix}_{i}.png".
    """
    # Clean & prep
    if column == 'duration_seconds':
        df = big_df.dropna(subset=['query_name', 'Run', 'duration_seconds']).copy()
        df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce')
        df = df.dropna(subset=['duration_seconds'])
        df['plot_value'] = df['duration_seconds']
        df['produced_results'] = big_df['produced_results'].astype(bool).fillna(False)
        y_label = "Execution Duration (s)"
        graph_title = "Query Execution Duration Plot"
        MAX_BAR = float(max_bar)
    elif column == 'http_requests':
        df = big_df.dropna(subset=['query_name', 'Run', 'http_requests']).copy()
        df['http_requests'] = pd.to_numeric(df['http_requests'], errors='coerce')
        df = df.dropna(subset=['http_requests'])
        df['plot_value'] = df['http_requests']
        df['produced_results'] = big_df['produced_results'].astype(bool).fillna(False)
        y_label = "HTTP Requests"
        graph_title = "HTTP Requests Plot"
        MAX_BAR = float(max_bar)

    # Stable ordering
    query_order = pd.Index(df['query_name']).drop_duplicates().tolist()
    run_order = pd.Index(df['Run']).drop_duplicates().tolist()

    # Even-ish split of queries
    query_batches = [batch.tolist() for batch in np.array_split(query_order, n_parts)]

    # Plot each batch
    for batch_idx, queries in enumerate(query_batches, start=1):
        if not queries:
            continue
        batch_df = df[df['query_name'].isin(queries)]
        bar_chart(batch_df, graph_title, batch_idx, run_order, y_label, MAX_BAR,
                  output_prefix=output_prefix,
                  bar_thinness=bar_thinness,
                  no_inner_spacing=no_inner_spacing,
                  group_width=group_width)


def shorten_label(q, limit=12):
    # Case 1: name contains "emi#" → keep prefix + digits before .rq
    if "emi#" in q:
        # Look for pattern emi#...<digits+letters>_ns.rq or .rq
        match = re.search(r"emi#[^0-9]*([0-9a-zA-Z]+)_ns\.rq", q)
        if not match:
            # fallback: try pattern before plain .rq if "_ns" missing
            match = re.search(r"emi#[^0-9]*([0-9a-zA-Z]+)\.rq", q)
        if match:
            return f"emi#{match.group(1)}"
        else:
            return "emi#???"

    # Case 2: name starts with "#" and is too long → truncate and add "..."
    elif re.match(r"^\d+", q) and len(q) > limit:
        return q[:limit-2] + "..."

    # Case 3: general truncation
    elif len(q) > limit:
        return q[:10] + " ..."
    
    # Default: leave as is
    return q


def bar_chart(batch_df, graph_title, batch_idx, run_order, y_label, MAX_BAR,
              output_prefix, bar_thinness, no_inner_spacing, group_width):
    """
    Take batch data, create a plot, and save to f"{output_prefix}_{batch_idx}.png".
    Shows ALL samples (no averaging), groups by query with sub-groups per Run,
    caps bars at MAX_BAR and adds '*' for capped values.
    """

    # Local order for this batch
    queries = pd.Index(batch_df['query_name']).drop_duplicates().tolist()
    n_queries = len(queries)
    n_runs = max(len(run_order), 1)

    # Layout
    x_base = np.arange(n_queries)           # one base slot per query
    run_slot = group_width / n_runs         # each run gets a slice inside the query slot

    fig, ax = plt.subplots(figsize=(12, 6)) # consistent width
    ax.set_title(f"{graph_title} - {batch_idx}")
    ax.set_xlabel("Query Name")
    ax.set_ylabel(y_label)
    ax.set_ylim(0, MAX_BAR * 1.1)
    ax.margins(x=0.005)

    legend_handles = {}

    for qi, q in enumerate(queries):
        q_base = x_base[qi]
        for rj, run in enumerate(run_order):
            subset = batch_df.loc[(batch_df['query_name'] == q) & (batch_df['Run'] == run)]
            vals = subset['plot_value'].tolist()
            if not vals:
                continue

            # run sub-slot within query group
            slot_center = q_base - (group_width / 2) + rj * run_slot + run_slot / 2
            slot_left   = slot_center - run_slot/2
            k = len(vals)

            if k == 1:
                xs = [slot_center]
                bar_width = run_slot * bar_thinness
            else:
                pad = 0 if no_inner_spacing else run_slot * 0.15
                usable = run_slot - 2 * pad
                spacing = usable / k
                xs = [slot_left + pad + (i + 0.5) * spacing for i in range(k)]
                bar_width = spacing * bar_thinness

            # cap + asterisk for over-threshold
            vals_capped = [min(v, MAX_BAR) for v in vals]
            over_mask = [v > MAX_BAR for v in vals]

            # hatching flags (True → hatched)
            produced_flags = subset['produced_results'].astype(bool).tolist() if 'produced_results' in subset.columns else [False] * len(vals)

            # draw bars one-by-one so each can have its own hatch
            palette = make_analogous_palette(len(run_order), base_hue_deg=200, spread_deg=90, s=0.7, l=0.60, pastel=1)
            run_colors = {run: palette[i % len(palette)] for i, run in enumerate(run_order)}

            bars = []
            for xi, yi, over, hatched in zip(xs, vals_capped, over_mask, produced_flags):
                b = ax.bar(
                    xi, yi,
                    width=bar_width,
                    hatch='///' if hatched else None,
                    color=run_colors[run],      # <- fixed, color-blind friendly color
                    alpha=0.85,                 # <- soften to a pastel feel
                    edgecolor='black',          # <- crisp outlines help with hatching & accessibility
                    linewidth=0.6,
                    label=None if run in legend_handles else str(run)
                )
                bars.append(b[0])

            if run not in legend_handles and bars:
                legend_handles[run] = bars[0]

            # asterisk for capped bars
            for xi, yi, over in zip(xs, vals_capped, over_mask):
                if over:
                    ax.text(xi, yi, '*', ha='center', va='bottom', fontsize=12, fontweight='bold')

    # X ticks per-query
    short_labels = [q if len(q) <= 12 else shorten_label(q) for q in queries]
    ax.set_xticks(x_base)
    ax.set_xticklabels(short_labels, rotation=45, ha="right")

    if batch_idx == 1 and legend_handles:
        runs_present = [r for r in run_order if r in legend_handles]  # only runs drawn in this batch
        run_patches = [
            Patch(facecolor=run_colors[r], edgecolor='black', linewidth=0.6, alpha=0.85, label=str(r))
            for r in runs_present
        ]

        hatch_patch = Patch(facecolor='white', edgecolor='black', hatch='///', label='Produced Results')

        ax.legend(
            run_patches + [hatch_patch],
            [str(r) for r in runs_present]  + ['Produced Results'],
            title="Federation Approach / Status",
            ncol=len(runs_present)-1,
            frameon=False,
            bbox_to_anchor=(-0.02, 1.25),  # adjust if you want higher/lower
            borderaxespad=0.0,
            columnspacing=0.8,
            handletextpad=0.4,
            loc='upper left'
        )

    plt.tight_layout()
    plt.savefig(f"{output_prefix}_{batch_idx}.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def scatter_plot(df, x_col, y_col, title, xlabel, ylabel):
    plt.figure(figsize=(10, 6))
    plt.scatter(df[x_col], df[y_col], color='salmon')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def error_bar_chart(df, x_col, y_col, yerr_col, title, xlabel, ylabel):
    plt.figure(figsize=(10, 6))
    plt.bar(df[x_col], df[y_col], yerr=df[yerr_col], capsize=5, color='lightgreen')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Summarize all .txt query run logs in a directory.")
    parser.add_argument("input_summaries", help="A list of summaries containing csv formatted data to visualize.")
    args = parser.parse_args()

    files = [
    "/EX1-17-9-25/summary.csv",
    "/EX2-13-10-25/summary.csv",
    "/EX3-16-10-25/summary.csv",
    "/EX4-24-10-25/summary.csv"
    ]
    labels = ["EX-1", "EX-2", "EX-3", "EX-4"]

    # Combine data from multiple experiments
    dfs = []
    for n in range(len(files)):
        df = pd.read_csv(args.input_summaries + files[n], header=0, skiprows=[1])  # same header rule
        df['Run'] = labels[n]  # tag each dataset
        dfs.append(df)
    combined_df = pd.concat(dfs)

    output_dir = os.path.dirname(os.getcwd() + "/figures/")
    try:
        os.makedirs(output_dir, exist_ok=False)
    except FileExistsError:
        pass
        print(f"Output directory {output_dir} already exists, continuing ...")

    split_data(combined_df,
        column='duration_seconds',
        n_parts=4,
        output_prefix="figures/query_duration_plot",
        max_bar=2700.0,   # y-axis cap
        bar_thinness=1,   # thinner bars
        no_inner_spacing=True,
        group_width=0.75)    # tighter groups horizontally
    
    split_data(combined_df,
        column='http_requests',
        n_parts=4,
        output_prefix="figures/http_requests_plot",
        max_bar=1000.0,   # y-axis cap
        bar_thinness=1,   # thinner bars
        no_inner_spacing=True,
        group_width=0.75)    # tighter groups horizontally

    # data = pd.concat(dfs, ignore_index=True)
    # line_chart(data, 'query_name', 'duration_seconds', 'Query Execution Time', 'Queries', 'Time (s)')
    # scatter_plot(data, 'query_name', 'results_count', 'Query Result Count', 'Queries', 'Results')
    # error_bar_chart(data, 'query_name', 'produced_results', 'ErrorMargin', 'Query Execution Time with Error Bars', 'Queries', 'Time (s)')

if __name__ == "__main__":
    main()
