import os

from plumtreeMessagesScript import plumtree_messages
from reliabilityScript import reliability
from totalTransmittedReceivedScript import messages_bytes
from averageBroadcastLatency import avg_latency
from graphFunctions import GraphBuilder

#start_name = "./results_plumtree/results-MacBook-Pro-de-Ema.local-{}.txt"
root = "./results/"
cyclon_eager_path = "cyclonEager/"
cyclon_plumtree_path = "cyclonPlumtree/"
hypar_eager_path = "hyparEager/"
hypar_plumtree_path = "hyparPlumtree/"
file_names = "results-Alexandres-MBP.lan-{}.txt"

latencyIndex = 0
reliabilityIndex = 1
transmittedIndex = 2
plumTreeIndex = 3

n_processes = 30
starting_port = 5000

cyclon_eager_results = []
cyclon_plumtree_results = []
hypar_eager_results = []
hypar_plumtree_results = []

for subdir, dirs, files in os.walk(root):
    if(subdir != "./results/"):
        path = subdir + "/" + file_names

        print("Analyzing files in", subdir)
        rel = reliability(path, n_processes, starting_port)
        transm = messages_bytes(path, n_processes, starting_port)
        lat = avg_latency(path, n_processes, starting_port)
        print()

        subdir += "/"

        if(subdir == root + cyclon_eager_path):
            cyclon_eager_results.append(lat)
            cyclon_eager_results.append(rel)
            cyclon_eager_results.append(transm)

        elif(subdir == root + cyclon_plumtree_path):
            plum = plumtree_messages(path, n_processes, starting_port)
            cyclon_plumtree_results.append(lat)
            cyclon_plumtree_results.append(rel)
            cyclon_plumtree_results.append(transm)
            cyclon_plumtree_results.append(plum)

        elif(subdir == root + hypar_eager_path):
            hypar_eager_results.append(lat)
            hypar_eager_results.append(rel)
            hypar_eager_results.append(transm)

        elif(subdir == root + hypar_plumtree_path):
            plum = plumtree_messages(path, n_processes, starting_port)
            hypar_plumtree_results.append(lat)
            hypar_plumtree_results.append(rel)
            hypar_plumtree_results.append(transm)
            hypar_plumtree_results.append(plum)

graphBuilder = GraphBuilder(cyclon_eager_results, cyclon_plumtree_results, hypar_eager_results, hypar_plumtree_results)

graphBuilder.create_latency_graph()
graphBuilder.create_reliability_graph()
graphBuilder.create_messages_bytes_graphs()