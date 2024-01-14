# load results from results.json file and use matplotlib to plot them in bar chart (each bar should represent one test case)
from matplotlib import pyplot as plt
import json

with open("celery_results.json") as f:
    celery_results = json.load(f)["dags_runs_time_in_seconds"]

with open("k8s_results.json") as f:
    k8s_results = json.load(f)["dags_runs_time_in_seconds"]

# plot results, so bars are in pairs (one from celery, one from k8s)
fig, ax = plt.subplots()
width = 0.35
x = [i for i in range(len(celery_results))]
ax.bar(x, celery_results, width, label="celery executor")
ax.bar([i + width for i in x], k8s_results, width, label="k8s executor")
ax.set_ylabel("Time [s]")
ax.set_xlabel("Test [n]")
ax.set_title("Execution time k8s vs celery")
ax.set_xticks([i + width / 2 for i in x])
ax.set_xticklabels([f"test {i + 1}" for i in range(len(celery_results))])
ax.legend()
plt.savefig("./images/results.png")
plt.show()
