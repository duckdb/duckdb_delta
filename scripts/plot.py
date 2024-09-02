import duckdb
import argparse

### Parse script parameters
parser = argparse.ArgumentParser(description='Plot the results in ./benchmark_results')
parser.add_argument('-p','--pattern', help='Pattern to match result csv files to', required=False, default='*.csv')
parser.add_argument('-w','--width', help='Width of graph, adjust to fit data', required=False, default=20)
args = vars(parser.parse_args())

### Parse Query Results
parse_benchmark_result_query = f"""
SELECT 
    parse_filename(name, true) as benchmark, 
    parse_filename(filename, true) as config, 
    avg(timing) as timing
FROM
    read_csv('benchmark_results/{args['pattern']}', filename=1, columns = {{
        'name': 'VARCHAR',
        'run': 'BIGINT',
        'timing': 'double'
    }}) 
GROUP BY 
    config, 
    benchmark 
ORDER BY 
    config, 
    benchmark
"""

benchmark_results = duckdb.execute(parse_benchmark_result_query).df()

### Plot graph
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["figure.figsize"] = [int(args['width']), 5]
fig = benchmark_results.pivot(index='benchmark', columns='config', values='timing').plot(kind='bar', title='', ylabel='runtime [s]').get_figure()
fig.savefig('benchmark_results/result.png')