class Node:
    def __init__(self, data):
        self.data = data
        self.next = None

class Queue:
    def __init__(self):
        self.front = None
        self.rear = None

    def iqueue(self):
        n1 = int(input("Enter data = "))
        n = Node(n1)

        if self.rear is None:
            self.front = n
            self.rear = n
        else:
            self.rear.next = n
            self.rear = n

    def dqueue(self):
        if self.front is None:
            print("Empty")
        elif self.front.next is None:
            self.front=None
        else:
            tmp = self.front
            self.front = tmp.next
            tmp = None
            

    def display(self):
        if self.front is None:
            print("Empty queue")
        else:
            tmp = self.front
            while tmp:
                print(tmp.data, end=' ')
                tmp = tmp.next
            print()

c = Queue()
while True:
    print("Enter the option \n1---Insert in queue \n2---Deletion in queue \n3---Display the queue \n4---Exit")
    r = int(input("Enter option:"))
    if r == 1:
        print("Insert in Queue")
        c.iqueue()
    elif r == 2:
        print("Deletion in queue")
        c.dqueue()
    elif r == 3:
        print("Display operation")
        c.display()
    elif r == 4:
        print("Exit")
        break
