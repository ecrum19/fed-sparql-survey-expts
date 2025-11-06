import pandas as pd
import argparse
import matplotlib.pyplot as plt
import numpy as np

# Load the data
def load_data(file_path):
    df = pd.read_csv(file_path, header=0, skiprows=[1])
    return df

# generate a figure of query execution times
def bar_chart(big_df):
  # Pivot so that each Run is a column
  pivot_df = big_df.pivot(index='query_name', columns='Run', values='duration_seconds')

  # Prepare bar positions
  x = np.arange(len(pivot_df.index))  # number of queries
  width = 0.2  # width of each bar

  fig, ax = plt.subplots(figsize=(12,6))

  for i, run in enumerate(pivot_df.columns):
      ax.bar(x + i*width, pivot_df[run], width, label=run)

  ax.set_xlabel("Query Name")
  ax.set_ylabel("Execution Duration (ms)")
  ax.set_title("Comparison of Query Execution Durations Across Runs")
  ax.set_xticks(x + width * (len(pivot_df.columns)-1)/2)
  ax.set_xticklabels(pivot_df.index, rotation=45, ha="right")
  ax.legend()

  plt.tight_layout()
  plt.show()


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
    print(combined_df)

    bar_chart(combined_df)

    # data = pd.concat(dfs, ignore_index=True)
    # line_chart(data, 'query_name', 'duration_seconds', 'Query Execution Time', 'Queries', 'Time (s)')
    # scatter_plot(data, 'query_name', 'results_count', 'Query Result Count', 'Queries', 'Results')
    # error_bar_chart(data, 'query_name', 'produced_results', 'ErrorMargin', 'Query Execution Time with Error Bars', 'Queries', 'Time (s)')

if __name__ == "__main__":
    main()
