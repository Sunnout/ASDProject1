start_name = "./results/results-MacBook-Pro-de-Ema.local-{}.txt"
n_processes = 30
starting_port = 5000


import datetime as dt

"""
msg_send_time = {'a' : dt.datetime.strptime("19:23:06,12345", '%H:%M:%S,%f').time(), 'b' : dt.datetime.strptime("19:23:06,12346", '%H:%M:%S,%f').time() }
msg_deliver_time = {'a' : dt.datetime.strptime("19:23:06,12365", '%H:%M:%S,%f').time(), 'b' : dt.datetime.strptime("19:23:06,12386", '%H:%M:%S,%f').time() }
"""

msg_send_time = {}
msg_deliver_time = {}

for port in range(starting_port, starting_port + n_processes):
    f = open(start_name.format(port), "r")

    for i in f:
        line = i.split(" ")
        if line[1].__contains__("BroadcastApp") and line[2].__contains__("Sending"):
            send_time = line[0].split("[")[2][:-1]
            send_time_obj = dt.datetime.strptime(send_time, '%H:%M:%S,%f').time()
            msg_send_time[line[3]] = send_time_obj
        elif line[1].__contains__("BroadcastApp") and line[2].__contains__("Received"):
            deliver_time = line[0].split("[")[2][:-1]
            deliver_time_obj = dt.datetime.strptime(deliver_time, '%H:%M:%S,%f').time()
            
            if not msg_deliver_time.get(line[3]): 
                msg_deliver_time[line[3]] = deliver_time_obj
            elif msg_deliver_time[line[3]] < deliver_time_obj:
                msg_deliver_time[line[3]] = deliver_time_obj

#print(len(msg_send_time))
#print(len(msg_deliver_time))

latency = {}
for key in msg_send_time:
    if msg_deliver_time.get(key):
        deliver_date = dt.datetime.combine(dt.date.today(), msg_deliver_time[key])
        send_date = dt.datetime.combine(dt.date.today(), msg_send_time[key])
        latency[key] = deliver_date - send_date

total_time = 0
for key in latency:
    total_time = total_time + latency[key].microseconds

avg_broadcast_latency = total_time / len(latency) / 1000000
    
print("Average Broadcast Latency: {}".format(avg_broadcast_latency))


import matplotlib.pyplot as plt

avg_latencies = [1, 0.5, 0.4, 0.1]


#Plot
font = {'fontname':'Arial'}
teamColours = ['navajowhite','orange','lightgreen','green']
fig, ax = plt.subplots()
ax = plt.bar(width=0.7, x=["Eager Push \nwith HyParView", "Eager Push \nwith Cyclon", "Plumtree \nwith HyParView", "Plumtree \nwith Cyclon"], height=avg_latencies, color=teamColours)
plt.title("Average Broadcast Latency", **font)
plt.axis([None, None, None, 2])
plt.yticks([])
xlocs, xlabs = plt.xticks()
xlocs=[0,1,2,3]
plt.xticks(xlocs, **font)
plt.xlabel("\n Threads: 32", **font)

for i, v in enumerate(avg_latencies):
    plt.text(xlocs[i] -0.16, v + 0.05, str("{:.2f}".format(v)))
    
plt.show()
#plt.savefig('../plots/speedupMapReduce.pdf', format='pdf')

    
    
    