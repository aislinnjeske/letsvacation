import matplotlib.pyplot as plt
import random

def readfile(filename, random=False):
    with open(filename) as f:
        lines = f.readlines()
        lines = [line[1:-2].split(",") for line in lines]
        lines.sort()
        lo, mid, hi = [], [], []
        for entry in lines:
            if entry[0][-1] is 'o':
                lo.append([entry[0][:-3], int(entry[1])])
            elif entry[0][-1] is 'd':
                mid.append([entry[0][:-4], int(entry[1])])
            elif entry[0][-1] is 'i':
                hi.append([entry[0][:-3], int(entry[1])])
        loLabels = [entry[0] for entry in lo]
        midLabels = [entry[0] for entry in mid]
        hiLabels = [entry[0] for entry in hi]
        
        lo = [entry[1] for entry in lo if entry[0] in hiLabels and entry[0] in midLabels]
        mid = [entry[1] for entry in mid if entry[0] in hiLabels and entry[0] in loLabels]
        hi = [entry[1] for entry in hi if entry[0] in midLabels and entry[0] in loLabels]
        labels = [label for label in loLabels if label in midLabels and label in hiLabels]
        
        if random:
            randomValues = random.sample(range(1, len(labels)), 20)
            return rList(lo, randomValues), rList(mid, randomValues), rList(hi, randomValues), rList(labels, randomValues)
        else:
            return lo, mid, hi, labels
    
def rList(l, randomValues):
    return [l[x] for x in randomValues]
    
def plot(lo, mid, hi, labels):
    #half = len(labels) // 2
    #plt.subplot(2,1,1)
    #plt.plot(lo[:half], '-o', label="Low Budget")
    #plt.plot(mid[:half], '-o', label="Mid Budget")
    #plt.plot(hi[:half], '-o', label="High Budget")
    #plt.xticks(range(half), labels[:half], rotation=60, horizontalalignment='right')
    #plt.tick_params(axis='x', which='major', labelsize=10)
    #plt.ylabel("Number of Days")
    #plt.grid()
    #plt.legend()
    
    #plt.subplot(2,1,2)
    #plt.plot(lo[half:], '-o', label="Low Budget")
    #plt.plot(mid[half:], '-o', label="Mid Budget")
    #plt.plot(hi[half:], '-o', label="High Budget")
    #plt.xticks(range(half), labels[half:], rotation=60, horizontalalignment='right')
    #plt.tick_params(axis='x', which='major', labelsize=10)
    #plt.xlabel("City Name")
    #plt.ylabel("Number of Days")
    ##plt.suptitle("With a Budget of $2000, How Long Can I Stay in Each City?")
    #plt.grid()
    #plt.legend()
    #plt.show()
    plt.plot(lo, '-o', label="Low Budget")
    plt.plot(mid, '-o', label="Mid Budget")
    plt.plot(hi, '-o', label="High Budget")
    plt.xticks(range(len(labels)), labels, rotation=45, horizontalalignment='right')
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.xlabel("City Name")
    plt.ylabel("Number of Days")
    plt.title("With a Budget of $5000, How Long Can I Stay in Each City?")
    plt.grid()
    plt.legend()
    plt.tight_layout()
    plt.show()
    
def calculateMostAndLeast(lo, mid, hi, labels):
    totals = [[lo[x] + mid[x] +  hi[x], lo[x], mid[x], hi[x], labels[x]] for x in range(len(labels))]
    totals.sort()
    top = totals[:10]
    bottom = totals[-10:]
    
    
    
if __name__ =="__main__":
    
    #random set of 20 for budget of 5000
    #lo, mid, hi, labels = readfile("../tripsForUser5KBudget.txt", True)
    #plot(lo, mid, hi, labels)
    
    #Calculate 10 most and 10 least cities fo 5000
    _, mid5, _, labels5 = readfile("../tripsForUser5KBudget.txt")
    _, mid2, _, _ = readfile("../tripsForUser2KBudget.txt")
    _, mid1, _, _ = readfile("../tripsForUser900Budget.txt")
    print(mid2)
    rV = random.sample(range(1, len(labels5)), 20)
    mid5, mid2, mid1 = rList(mid5, rV), rList(mid2, rV), rList(mid1, rV)
    print(mid2)
    labels5 = rList(labels5, rV)
    
    plt.plot(mid5, '-o', label="Mid 5K Budget")
    plt.plot(mid2, '-o', label="Mid 2K Budget")
    plt.plot(mid1, '-o', label="Mid 900 Budget")
    plt.xticks(range(len(labels5)), labels5, rotation=45, horizontalalignment='right')
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.xlabel("City Name")
    plt.ylabel("Number of Days")
    plt.title("With Different Budgets, How Long Can I Stay in Each City?")
    plt.grid()
    plt.legend()
    plt.tight_layout()
    plt.show()
    
    
    
    #calculateMostAndLeast(lo, mid, hi, labels)
    
    #print(output)
    #plot(lo, mid, hi, labels)
