

def push():
    n=input("enter element = ")
    if len(stack) == 0:
        stack.append(n)
    else:
        stack.insert(0,n)
    print(n,"is inserted in stack")
def pop():
    if len(stack)==0:
        print("stack is empty")
    else:
        print(stack[0],"is deleted")
        del stack[0]
def display():
    if len(stack)==0:
        print("Empty")
    else:
        for ele in stack:
            print(ele)
    print("top element is",stack[0])
stack=list()
while(1):
    print("Enter the operation from below \n1 --Push operation  \n2 --Pop Operation  \n3 --Display Operation \n4 --Enter any key tpo exit")
    r=input("Enter any operation")
    if r =='1':
        print("Push Opearion")
        push()
    elif r =='2':
        print("Pop operation")
        pop()
    elif r == '3':
        print("display operation")
        display()
    else:
        print("exit")
        break
          
         
         
          