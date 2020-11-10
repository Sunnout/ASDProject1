#start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
start_name = "./results/results-MacBook-Pro-de-Ema.local-{}.txt"
n_processes = 30
starting_port = 5000

messagesReceived = 0
messagesSent = 0

for port in range(starting_port, starting_port + n_processes):
    f = open(start_name.format(port), "r")

    for i in f:
        line = i.split(" ")
        if(line[1].__contains__("BroadcastApp")):
            if(line[2].__contains__("Received")):
                messagesReceived = messagesReceived + 1

            elif(line[2].__contains__("Sending")):
                messagesSent = messagesSent + 1

print("Received ", messagesReceived)
print("Sent", messagesSent)

avg_broad_reliability = messagesReceived / (messagesSent * n_processes) * 100

print("Average Broadcast Reliability: {}% ".format(avg_broad_reliability))

import matplotlib.pyplot as plt
avg_reliabilities = [0.9, 0.8, 0.98, 0.99]

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
    
plt.show()
#plt.savefig('plot.pdf', format='pdf')
 
