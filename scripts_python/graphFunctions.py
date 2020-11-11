import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
sb.set()

class GraphBuilder:

    def __init__(self, cyclon_eager_results, cyclon_plumtree_results, hypar_eager_results, hypar_plumtree_results):
        self.cyclon_eager_results = cyclon_eager_results
        self.cyclon_plumtree_results = cyclon_plumtree_results
        self.hypar_eager_results = hypar_eager_results
        self.hypar_plumtree_results = hypar_plumtree_results

        self.latencyIndex = 0
        self.reliabilityIndex = 1
        self.transmittedIndex = 2
        self.plumTreeIndex = 3

    def create_latency_graph(self):
        latencies = []
        latencies.append(self.cyclon_eager_results[self.latencyIndex])
        latencies.append(self.cyclon_plumtree_results[self.latencyIndex])
        latencies.append(self.hypar_eager_results[self.latencyIndex])
        latencies.append(self.hypar_plumtree_results[self.latencyIndex])

        font = {'fontname':'Arial'}
        colours = ['navajowhite','orange','lightgreen','green']
        _ , ax = plt.subplots()
        x = ["Cyclon \nwith Eager Push", "Cyclon \nwith Plumtree", "HyParView \nwith Eager Push", "HyParView \nwith Plumtree"]
        ax = plt.bar(width=0.7, x=x, height=latencies, color=colours)
        
        plt.title("Average Latencies (ms)", **font)
        maxVal = max(latencies)
        plt.axis([None, None, None, maxVal + 0.05 * maxVal])
        plt.yticks([])
        xlocs, _ = plt.xticks()
        xlocs=[0,1,2,3]
        plt.xticks(xlocs, **font)

        for i, v in enumerate(latencies):
            if(latencies[i] >= 100):
                plt.text(xlocs[i] - 0.22, v + 1, str("{:.2f}".format(v)))
            else:
                plt.text(xlocs[i] - 0.17, v + 1, str("{:.2f}".format(v)))

        plt.savefig('./graphs/latencies.pdf', format='pdf')

    def create_reliability_graph(self):
        reliabilities = []
        reliabilities.append(self.cyclon_eager_results[self.reliabilityIndex])
        reliabilities.append(self.cyclon_plumtree_results[self.reliabilityIndex])
        reliabilities.append(self.hypar_eager_results[self.reliabilityIndex])
        reliabilities.append(self.hypar_plumtree_results[self.reliabilityIndex])

        font = {'fontname':'Arial'}
        colours = ['navajowhite','orange','lightgreen','green']
        _ , ax = plt.subplots()
        x = ["Cyclon \nwith Eager Push", "Cyclon \nwith Plumtree", "HyParView \nwith Eager Push", "HyParView \nwith Plumtree"]
        ax = plt.bar(width=0.7, x=x, height=reliabilities, color=colours)
        
        plt.title("Average Reliabilities (%)", **font)
        maxVal = max(reliabilities)
        plt.axis([None, None, None, maxVal + 0.05 * maxVal])
        plt.yticks([])
        xlocs, _ = plt.xticks()
        xlocs=[0,1,2,3]
        plt.xticks(xlocs, **font)

        for i, v in enumerate(reliabilities):
            if(reliabilities[i] == 100):
                plt.text(xlocs[i] - 0.22, v + 1, str("{:.2f}".format(v)))
            else:
                plt.text(xlocs[i] - 0.17, v + 1, str("{:.2f}".format(v)))

        plt.savefig('./graphs/reliabilities.pdf', format='pdf')

    def create_messages_bytes_graphs(self):
        cyclon_eager = []
        cyclon_plum = []
        hypar_eager = []
        hypar_plum = []

        mt, mr, bt, br = self.cyclon_eager_results[self.transmittedIndex]
        cyclon_eager.append(mt)
        cyclon_eager.append(mr)
        cyclon_eager.append(bt)
        cyclon_eager.append(br)

        mt, mr, bt, br =  self.cyclon_plumtree_results[self.transmittedIndex]
        cyclon_plum.append(mt)
        cyclon_plum.append(mr)
        cyclon_plum.append(bt)
        cyclon_plum.append(br)

        mt, mr, bt, br = self.hypar_eager_results[self.transmittedIndex]
        hypar_eager.append(mt)
        hypar_eager.append(mr)
        hypar_eager.append(bt)
        hypar_eager.append(br)

        mt, mr, bt, br =  self.hypar_plumtree_results[self.transmittedIndex]
        hypar_plum.append(mt)
        hypar_plum.append(mr)
        hypar_plum.append(bt)
        hypar_plum.append(br)

        self.__create_messages_graph(cyclon_eager[0:2], cyclon_plum[0:2], hypar_eager[0:2], hypar_plum[0:2])
        self.__create_bytes_graph(cyclon_eager[2:], cyclon_plum[2:], hypar_eager[2:], hypar_plum[2:])

    def __create_messages_graph(self, cyclon_eager, cyclon_plum, hypar_eager, hypar_plum):
        n = 2
        ind = np.arange(n)
        width = 0.2

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, cyclon_eager, width, color='navajowhite')
        rects2 = ax.bar(ind, cyclon_plum, width, color='orange')
        rects3 = ax.bar(ind + width, hypar_eager, width, color='lightgreen')
        rects4 = ax.bar(ind + 2 * width, hypar_plum, width, color='green')
        ax.set_title('Messages Transmitted and Received')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Messages\nTransmitted', 'Messages\nReceived'))
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Cyclon\nEager Push", "Cyclon\nPlumtree", "HyparView\nEager Push", "HyparView\nPlumtree"), fontsize='x-small')
        plt.savefig('./graphs/messages.pdf', format='pdf')

    def __create_bytes_graph(self, cyclon_eager, cyclon_plum, hypar_eager, hypar_plum):
        n = 2
        ind = np.arange(n)
        width = 0.2

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, cyclon_eager, width, color='navajowhite')
        rects2 = ax.bar(ind, cyclon_plum, width, color='orange')
        rects3 = ax.bar(ind + width, hypar_eager, width, color='lightgreen')
        rects4 = ax.bar(ind + 2 * width, hypar_plum, width, color='green')
        ax.set_title('Bytes Transmitted and Received')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Bytes\nTransmitted', 'Bytes\nReceived'))
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Cyclon\nEager Push", "Cyclon\nPlumtree", "HyparView\nEager Push", "HyparView\nPlumtree"), fontsize='x-small')
        plt.savefig('./graphs/bytes.pdf', format='pdf')