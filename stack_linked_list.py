
class Node:
    def __init__(self,data):
        self.data=data
        self.next=None
class Stack:
    def __init__(self):
        self.top=None
    def push(self):
        n=int(input("Enter the data = "))
        new=Node(n)
       # new=Node(n)
        if self.top is None:
           
            self.top=new
            self.top.next=None
        else:
            
            new.next=self.top
            self.top=new
    def pop(self):
        if self.top is None:
            print("empty")
        elif self.top.next is None:
            self.top=None
        else:
            temp=self.top
            self.top=temp.next
            temp=None
    def display(self):
        if self.top is None:
            print("empty list")
        else:
            temp=self.top
            while(temp):
                print(temp.data)
                temp=temp.next
s=Stack()
while(1):
    print("Enter the operation from below \n1 --Push operation  \n2 --Pop Operation  \n3 --Display Operation \n4 --Enter any key tpo exit")
    r=input("Enter any operation")
    if r =='1':
        print("Push Opearion")
        s.push()
    elif r =='2':
        print("Pop operation")
        s.pop()
    elif r == '3':
        print("display operation")
        s.display()
    else:
        print("exit")
        break
          
         
         
          
            
        
            
            
            
            
    