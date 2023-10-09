# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 15:06:03 2023

@author: kanupriyag
"""




class Node:
    def __init__(self,data):
        self.data=data
        self.next=None
class SSL:
    def __init__(self):
        self.head=None
    def display(self):
        temp=self.head
        if temp is None:
            print("empty")
        while temp:
            print(temp.data,end='-->')
            temp=temp.next
    def insert_beg(self,data):
        n=Node(data)
        temp=self.head
        n.next=temp
        temp.prev=n
        self.head=n
    def insert_end(self,data):
        n=Node(data)
        temp=self.head
        while temp.next is not None:
            temp=temp.next
        n.prev=temp
        temp.next=n
    def insert_pos(self,data,pos):
        n=Node(data)
        temp=self.head

        for i in range(1,pos-1):
            temp=temp.next
            
        n.prev=temp
        n.data=data
        n.next=temp.next
        temp.next.prev=n
        temp.next=n
            
        
    
            
c=SSL()
n1=Node(2)
c.head=n1
n2=Node(3)
n1.next=n2
n1.prev=None
n2.prev=n1
c.display()  # This will print: 2 --> 3
print()
c.insert_pos(23,2)
#c.insert_beg(4)
#c.display()  # Th
#c.insert_end(34)
c.display()