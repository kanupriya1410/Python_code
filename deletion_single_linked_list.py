class Node:
    def __init__(self,data):
        self.data=data
        self.next=None
class SLL:
    def __init__(self):
        self.head=None
        
    def delete_beg(self):
        temp=self.head
        self.head=temp.next
        temp.next=None
        
        
    def delete_end(self):
        temp=self.head.next
        prev=self.head
        while temp.next is not None:
            temp=temp.next
            prev=prev.next
        prev.next=None
    
    
    def delete_pos(self,pos):
        temp=self.head.next
        prev=self.head
        for i in range(0,pos-1):
            temp=temp.next
            prev=prev.next
        prev.next=temp.next
        temp.next=None

    def duplicate_del(self):
        temp = self.head  # Start from the head
        while temp is not None and temp.next is not None:
            if temp.data == temp.next.data:
                # Duplicate found, remove it
                temp.next = temp.next.next
            else:
                # No duplicate found, move to the next node
                temp = temp.next
    
    
    
    def duplicates_del(self):
        temp = self.head  # Start from the head
        
        while temp is not None and temp.next is not None:
            if temp.data == temp.next.data:
                # Duplicate found, remove it
                temp.next = temp.next.next
            else:
                # No duplicate found, move to the next node
                temp = temp.next

        
        
        
        
        
        
        
        
    def display(self):
        temp=self.head
        if temp is None:
            print("empty")
        while(temp):
            print(temp.data,end='-->')
            temp=temp.next
s=SLL()
n1=Node(23)
s.head=n1
n2=Node(24)
n1.next=n2
n3=Node(24)
n2.next=n3
n4=Node(26)
n3.next=n4
n5=Node(27)
n4.next=n5
#s. delete_end()
#s.delete_beg()
#s.delete_pos(2)
s.duplicates_del()
s.display()
