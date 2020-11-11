import datetime as dt

def avg_latency(start_name, n_processes, starting_port, to_print=False):
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

    latency = {}

    for key in msg_send_time:
        if msg_deliver_time.get(key):
            deliver_date = dt.datetime.combine(dt.date.today(), msg_deliver_time[key])
            send_date = dt.datetime.combine(dt.date.today(), msg_send_time[key])
            latency[key] = deliver_date - send_date

    total_time = 0

    for key in latency:
        total_time = total_time + latency[key].microseconds

    avg_broadcast_latency = total_time / len(latency) / 1000

    if(to_print):
        print("Average Broadcast Latency: {:.2f} ms".format(avg_broadcast_latency))
        print("")
        
    return avg_broadcast_latency

start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
n_processes = 5
starting_port = 5000

#avg_latency(start_name, n_processes, starting_port)