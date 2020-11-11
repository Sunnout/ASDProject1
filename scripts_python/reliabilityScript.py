def reliability(start_name, n_processes, starting_port, to_print=False):
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

    avg_broad_reliability = messagesReceived / (messagesSent * n_processes) * 100
    
    if(to_print):
        print('Reliability Analysis:')
        print()
        print("Total Messages Received:", messagesReceived)
        print("Total Messages Sent:", messagesSent)
        print("Average Broadcast Reliability: {:.2f}% ".format(avg_broad_reliability))
        print()
        
    return avg_broad_reliability

start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
n_processes = 5
starting_port = 5000

#reliability(start_name, n_processes, starting_port)