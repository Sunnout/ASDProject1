import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
sb.set()

class GraphBuilder:

    FONT = {'fontname':'Arial'}
    COLOURS = ['navajowhite','orange','lightgreen','green']

    RELIABILITY_FOLDER = 'reliabilities/'
    LATENCY_FOLDER = 'latencies/'

    EAGER_CYCLON_TITLE = 'Eager Push with Cyclon'
    EAGER_HYPAR_TITLE = 'Eager Push with HyParView'
    PLUM_CYCLON_TITLE = 'Plumtree with Cyclon'
    PLUM_HYPAR_TITLE = 'Plumtree with HyParView'

    S_MSG_B_TIME_TITLE = 'Small Messages with big broadcast interval'
    B_MSG_B_TIME_TITLE = 'Big Messages with big broadcast interval'
    S_MSG_S_TIME_TITLE = 'Small Messages with small broadcast interval'
    B_MSG_S_TIME_TITLE = 'Big Messages with small broadcast interval'

    EAGER_CYCLON_FILE = 'eager_cyclon.pdf'
    EAGER_HYPAR_FILE = 'eager_hypar.pdf'
    PLUM_CYCLON_FILE = 'plum_cyclon.pdf'
    PLUM_HYPAR_FILE = 'plum_hypar.pdf'

    S_MSG_B_TIME_FILE = 'small_mess_big_time.pdf'
    B_MSG_B_TIME_FILE = 'big_mess_big_time.pdf'
    S_MSG_S_TIME_FILE = 'small_mess_small_time.pdf'
    B_MSG_S_TIME_FILE = 'big_mess_small_time.pdf'

    def __init__(self, results, project_path):
        self.eager_cyclon_results = [results[0], results[1], results[2], results[3]]
        self.eager_hypar_results = [results[4], results[5], results[6], results[7]]
        self.plumtree_cyclon_results = [results[8], results[9], results[10], results[11]]
        self.plumtree_hypar_results = [results[12], results[13], results[14], results[15]]

        self.n_combs_protocols = 4
        self.n_combs_parameters = 4

        self.latency_index = 0
        self.reliability_index = 1
        self.transmitted_index = 2
        self.plumTree_index = 3

        self.m_T_index = 0
        self.M_T_index = 1
        self.m_t_index = 2
        self.M_t_index = 3

        self.graph_path = project_path + '/graphs/'

    def graphs_same_protocol(self, protocol_comb):
        data = []
        x = ["Small Messages\nBig Time", "Big Messages\nBig Time", "Small Messages\nSmall Time", "Big Messages\nSmall Time"]
        latencies = []
        reliabilities = []

        if(protocol_comb == 0):
            data = self.eager_cyclon_results
            title = self.EAGER_CYCLON_TITLE
            file_name = self.EAGER_CYCLON_FILE

        elif(protocol_comb == 1):
            data = self.eager_hypar_results
            title = self.EAGER_HYPAR_TITLE
            file_name = self.EAGER_HYPAR_FILE

        elif(protocol_comb == 2):
            data = self.plumtree_cyclon_results
            title = self.PLUM_CYCLON_TITLE
            file_name = self.PLUM_CYCLON_FILE

        else:
            data = self.plumtree_hypar_results
            title = self.PLUM_HYPAR_TITLE
            file_name = self.PLUM_HYPAR_FILE

        for results in data:
            latencies.append(results[self.latency_index])
            reliabilities.append(results[self.reliability_index])

        self.__create_graph(x, latencies, 'Latency: ' + title, 'ms', self.LATENCY_FOLDER, file_name)
        self.__create_graph(x, reliabilities, 'Reliability: ' + title, '%', self.RELIABILITY_FOLDER, file_name)

    def graphs_between_protocols(self, parameters_comb):
        x = ["Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"]
        latencies = []
        reliabilities = []

        if(parameters_comb == 0):
            title = self.S_MSG_B_TIME_TITLE
            file_name = self.S_MSG_B_TIME_FILE

        elif(parameters_comb == 1):
            title = self.B_MSG_B_TIME_TITLE
            file_name = self.B_MSG_B_TIME_FILE

        elif(parameters_comb == 2):
            title = self.S_MSG_S_TIME_TITLE
            file_name = self.S_MSG_S_TIME_FILE

        elif(parameters_comb == 3):
            title = self.B_MSG_S_TIME_TITLE
            file_name = self.B_MSG_S_TIME_FILE

        latencies.append(self.eager_cyclon_results[parameters_comb][self.latency_index])
        latencies.append(self.eager_hypar_results[parameters_comb][self.latency_index])
        latencies.append(self.plumtree_cyclon_results[parameters_comb][self.latency_index])
        latencies.append(self.plumtree_hypar_results[parameters_comb][self.latency_index])

        reliabilities.append(self.eager_cyclon_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.eager_hypar_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.plumtree_cyclon_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.plumtree_hypar_results[parameters_comb][self.reliability_index])

        self.__create_graph(x, latencies, 'Latency: ' + title, 'ms', self.LATENCY_FOLDER, file_name)
        self.__create_graph(x, reliabilities, 'Reliability: ' + title, '%', self.RELIABILITY_FOLDER, file_name)

    def __create_graph(self, x, data, title, units, folder, file_name):
        _ , ax = plt.subplots()
        ax = plt.bar(width=0.7, x=x, height=data, color=self.COLOURS)

        if(units != ''):
            title += " ({})".format(units)

        plt.title(title, **self.FONT)
        maxVal = max(data)
        plt.axis([None, None, None, maxVal + 0.05 * maxVal])
        plt.yticks([])
        xlocs, _ = plt.xticks()
        xlocs=[0,1,2,3]
        plt.xticks(xlocs, **self.FONT)

        for i, v in enumerate(data):
            if(data[i] >= 100):
                plt.text(xlocs[i] - 0.22, v + 1, str("{:.2f}".format(v)))
            else:
                plt.text(xlocs[i] - 0.17, v + 1, str("{:.2f}".format(v)))

        plt.savefig(self.graph_path + folder + file_name, format='pdf')

"""
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

"""

    #VER LIMITES DE ESCRITORES DE FICHEIROS
    #FAZER TUNEL SSH
    #meu porto 3000 a um storage com porto 3000
    #Tunel aberto no porto 3000
    #Com esta ligação, podemos correr localhost:3000 e ver o overview dashboard
    #Vemos o estado das máquinas todas

    #Podemos enviar mensagens por ligações abertas por outro peer, parâmetro connection do send, 
    #meter uma constante para que a mensagem seja enviado pelo incoming channel
        