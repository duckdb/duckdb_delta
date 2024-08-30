import duckdb

### Parse Query Results
parse_benchmark_result_query = """
SELECT 
    parse_filename(name, true) as benchmark, 
    parse_filename(filename, true) as config, 
    avg(timing) as timing
FROM
    read_csv('benchmark_results/*.csv', filename=1) 
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

plt.rcParams["figure.figsize"] = [10, 5]
fig = benchmark_results.pivot(index='benchmark', columns='config', values='timing').plot(kind='bar', title='', ylabel='runtime [s]').get_figure()
fig.savefig('benchmark_results/result.png')