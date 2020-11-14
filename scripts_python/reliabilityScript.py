import os

def reliability(start_name, n_processes, n_runs, combination, to_print=False):
    messages_received = []
    messages_sent = []

    for run in range(n_runs):
        messages_received.append(0)
        messages_sent.append(0)

    for proc in range(n_processes):
        progressBar(proc, n_processes)
        for run in range(n_runs):
            f = open(start_name.format(proc, combination, run+1), "r")
            non_dup_received_msgs = set()

            for i in f:
                line = i.split(" ")

                if(line[1].__contains__("BroadcastApp")):
                    if(line[2].__contains__("Received")):
                        non_dup_received_msgs.add(line[3])

                    elif(line[2].__contains__("Sending")):
                        messages_sent[run] += 1

            messages_received[run] += len(non_dup_received_msgs)

    print("Progress: [------------------->] 100%", end='\n')
    total_reliability = 0

    for run in range(n_runs):
        r = messages_received[run] / (messages_sent[run] * n_processes) * 100
        total_reliability = total_reliability + r

    avg_broad_reliability = total_reliability / n_runs
    avg_messages_received = sum(messages_received) / n_runs
    avg_messages_sent = sum(messages_sent) / n_runs
    
    if(to_print):
        print('Reliability Analysis:')
        print("Avg Messages Received:", avg_messages_received)
        print("Avg Messages Sent:", avg_messages_sent)
        print("Avg Broadcast Reliability: {:.2f}% ".format(avg_broad_reliability))
        print()
        
    return avg_broad_reliability

def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow   = '-' * int(percent/100 * barLength - 1) + '>'
    spaces  = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')

"""
start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
n_processes = 5
starting_port = 5000

#reliability(start_name, n_processes, starting_port)

"""