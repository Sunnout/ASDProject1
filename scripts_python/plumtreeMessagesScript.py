def plumtree_messages(start_name, n_processes, starting_port, to_print=False):
    sentGossip = 0
    sentGraft = 0
    sentPrune = 0
    sentIHave = 0
    receivedGossip = 0
    receivedGraft = 0
    receivedPrune = 0
    receivedIHave = 0

    for port in range(starting_port, starting_port + n_processes):
        f = open(start_name.format(port), "r")
        finalSentGossip = 0
        finalSentGraft = 0
        finalSentPrune = 0
        finalSentIHave = 0
        finalReceivedGossip = 0
        finalReceivedGraft = 0
        finalReceivedPrune = 0
        finalReceivedIHave = 0

        for i in f:
            line = i.split(" ")

            if(i.__contains__('Sent Gossip Msgs:')):
                finalSentGossip = int(line[3])

            elif(i.__contains__('Sent Graft Msgs:')):
                finalSentGraft = int(line[3])

            elif(i.__contains__('Sent Prune Msgs:')):
                finalSentPrune = int(line[3])

            elif(i.__contains__('Sent IHave Msgs:')):
                finalSentIHave = int(line[3])

            elif(i.__contains__('Received Gossip Msgs:')):
                finalReceivedGossip = int(line[3])

            elif(i.__contains__('Received Graft Msgs:')):
                finalReceivedGraft = int(line[3])

            elif(i.__contains__('Received Prune Msgs:')):
                finalReceivedPrune = int(line[3])

            elif(i.__contains__('Received IHave Msgs:')):
                finalReceivedIHave = int(line[3])

        sentGossip += finalSentGossip
        sentGraft += finalSentGraft
        sentPrune += finalSentPrune
        sentIHave += finalSentIHave
        receivedGossip += finalReceivedGossip
        receivedGraft += finalReceivedGraft
        receivedPrune += finalReceivedPrune
        receivedIHave += finalReceivedIHave

    totalSent = sentGossip + sentGraft + sentPrune + sentIHave
    percentageGossip = (sentGossip / totalSent) * 100
    percentageGraft = (sentGraft / totalSent) * 100
    percentagePrune = (sentPrune / totalSent) * 100
    percentageIHave = (sentIHave / totalSent) * 100

    if(to_print):
        print('PlumTree Messages:')
        print()
        print('Sent Gossip Messages: ', sentGossip)
        print('Sent Graft Messages: ', sentGraft)
        print('Sent Prune Messages: ', sentPrune)
        print('Sent IHave Messages: ', sentIHave)
        print()
        print('Received Gossip Messages: ', receivedGossip)
        print('Received Graft Messages: ', receivedGraft)
        print('Received Prune Messages: ', receivedPrune)
        print('Received IHave Messages: ', receivedIHave)
        print()
        print('Percentage Gossip Messages: {:.2f}'.format(percentageGossip))
        print('Percentage Graft Messages: {:.2f}'.format(percentageGraft))
        print('Percentage Prune Messages: {:.2f}'.format(percentagePrune))
        print('Percentage IHave Messages: {:.2f}'.format(percentageIHave))
        print()

    return totalSent, percentageGossip, percentageGraft, percentagePrune, percentageIHave

start_name = "./results/hyparPlumtree/results-Alexandres-MBP.lan-{}.txt"
n_processes = 30
starting_port = 5000

plumtree_messages(start_name, n_processes, starting_port, True)