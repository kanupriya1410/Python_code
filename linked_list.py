#creation of single link list and display in linked list


class Node:
    def __init__(self, data):
        self.data = data
        self.next = None

class SLL:
    def __init__(self):
        self.head = None
    
    def insert_beg(self,data):
        nb=Node(data)
        nb.next=self.head
        self.head=nb
        
    def insert_end(self,data):
        ne=Node(data)
        temp=self.head
        while(temp.next):
            temp=temp.next
        temp.next=ne
    def count_nodes(self):
        count=0
        temp=self.head
        while temp is not None:
            count+=1
            temp=temp.next
        print(f'total nodes = {count}')
    def Reverse_list(self):
     current=self.head
     prev=None
     while current is not None:
         next_node=current.next
         current.next=prev
         prev=current
         current=next_node
     self.head=prev
     
        
        
    def insert_pos(self,pos,data):
       np=Node(data)
       temp=self.head
       for i in range(pos-1):
           temp=temp.next
       np.data=data
       np.next=temp.next
       temp.next=np
         
             
    def display(self):
        temp = self.head
        if temp is None:
            print("Empty")
            #return
        while temp:
            print(temp.data, end=" -> ")
            temp = temp.next
        #print("None")

# Create an empty singly linked list
my_list = SLL()

# Create nodes and link them
c1 = Node(10)
my_list.head = c1
c2 = Node(12)
c1.next = c2
c3=Node(23)
c2.next=c3
c4=Node(34)
c3.next=c4
#my_list.insert_beg(5)
#my_list.insert_end(88)
#my_list.insert_pos(4,78)

# Display the linked list
my_list.display()

my_list.count_nodes()
my_list.Reverse_list()
my_list.display()



#____________________________________________________
#insert the element in first,last and end position





