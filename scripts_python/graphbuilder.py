import matplotlib.pyplot as plt
from plumtreeMessagesScript import plumtree_messages
from reliabilityScript import reliability
from totalTransmittedReceivedScript import messages_bytes
from averageBroadcastLatency import avg_latency

#start_name = "./results_plumtree/results-MacBook-Pro-de-Ema.local-{}.txt"
start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
n_processes = 5
starting_port = 5000

#plumtree_messages(start_name, n_processes, starting_port)
reliability(start_name, n_processes, starting_port)
messages_bytes(start_name, n_processes, starting_port)
avg_latency(start_name, n_processes, starting_port)

"""
#Plot
font = {'fontname':'Arial'}
colours = ['navajowhite','orange','lightgreen','green']
fig, ax = plt.subplots()
ax = plt.bar(width=0.7, x=["Eager Push \nwith HyParView", "Eager Push \nwith Cyclon", "Plumtree \nwith HyParView", "Plumtree \nwith Cyclon"], height=avg_reliabilities, color=colours)
plt.title("Time Speedup with light jobs vs. heavy jobs", **font)
plt.axis([None, None, None, 3])
plt.yticks([])
xlocs, xlabs = plt.xticks()
xlocs=[0,1,2,3]
plt.xticks(xlocs, **font)
plt.xlabel("\n Threads: 32", **font)

for i, v in enumerate(avg_reliabilities):
    plt.text(xlocs[i] -0.16, v + 0.05, str("{:.2f}".format(v)))

plt.savefig('plot.pdf', format='pdf')
"""