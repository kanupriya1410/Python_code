# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 15:06:03 2023

@author: kanupriyag
"""




class Node:
    def __init__(self,data):
        self.data=data
        self.next=None
        self.prev=None
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
    def del_beg(self):
        temp=self.head
        self.head=temp.next
        temp.next=None
        self.head.prev=None
    def del_end(self):
        before=self.head
        temp=self.head.next
        while temp.next is not None:
            temp=temp.next
            before=before.next
        before.next=None
        temp.prev=None
    def del_pos(self,pos):
        before=self.head
        temp=self.head.next
        for i in range(1,pos-1):
            temp=temp.next
        before.next=temp.next
        temp.next.prev=before
        
        
c=SSL()
n1=Node(2)
c.head=n1
n2=Node(3)
n1.next=n2
n1.prev=None
n2.prev=n1
n3=Node(4)
n2.next=n3
n4=Node(5)
n3.next=n4
c.display()  # This will print: 2 --> 3
print()

c.del_pos(2)
#c.insert_beg(4)
#c.display()  # Th
#c.insert_end(34)
c.display()