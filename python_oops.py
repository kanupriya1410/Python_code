# Q1. Write a Python program to demonstrate multiple inheritance.
# 1. Employee class has 3 data members EmployeeID, Gender (String), Salary and
# PerformanceRating(Out of 5) of type int. It has a get() function to get these details from
# the user.
# 2. JoiningDetail class has a data member DateOfJoining of type Date and a function
# getDoJ to get the Date of joining of employees.
# 3. Information Class uses the marks from Employee class and the DateOfJoining date
# from the JoiningDetail class to calculate the top 3 Employees based on their Ratings
# and then Display, using readData, all the details on these employees in Ascending
# order of their Date Of Joining.


from datetime import date

class Employee:
    def __init__(self):
        self.EmployeeID = 0
        self.Gender = ""
        self.Salary = 0
        self.PerformanceRating = 0

    def get(self):
        self.EmployeeID = int(input("Enter Employee ID: "))
        self.Gender = input("Enter Gender: ")
        self.Salary = float(input("Enter Salary: "))
        self.PerformanceRating = int(input("Enter Performance Rating (out of 5): "))

class JoiningDetail:
    def __init__(self):
        self.DateOfJoining = date.today()

    def getDoJ(self):
        year = int(input("Enter Joining Year (YYYY): "))
        month = int(input("Enter Joining Month (MM): "))
        day = int(input("Enter Joining Day (DD): "))
        self.DateOfJoining = date(year, month, day)

class Information(Employee, JoiningDetail):
    def __init__(self):
        Employee.__init__(self)
        JoiningDetail.__init__(self)
        self.employees = []

    def readData(self):
        n = int(input("Enter the number of employees: "))
        for _ in range(n):
            emp = Information()
            emp.get()
            emp.getDoJ()
            self.employees.append(emp)

    def displayTopEmployees(self):
        top_employees = sorted(self.employees, key=lambda x: x.PerformanceRating, reverse=True)[:3]
        top_employees = sorted(top_employees, key=lambda x: x.DateOfJoining)
        
        print("\nTop 3 Employees based on Performance Rating and Date of Joining:")
        for emp in top_employees:
            print(f"Employee ID: {emp.EmployeeID}")
            print(f"Gender: {emp.Gender}")
            print(f"Salary: {emp.Salary}")
            print(f"Performance Rating: {emp.PerformanceRating}")
            print(f"Date of Joining: {emp.DateOfJoining}")
            print()

# Main program
info = Information()
info.readData()
info.displayTopEmployees()

# Q.2 Write a Python program to demonstrate Polymorphism.
# 1. Class Vehicle with a parameterized function Fare, that takes input value as fare and
# returns it to calling Objects.
# 2. Create five separate variables Bus, Car, Train, Truck and Ship that call the Fare
# function.
# 3. Use a third variable TotalFare to store the sum of fare for each Vehicle Type.
# 4. Print the TotalFare.






class Vehicle:
    def __init__(self, fare):
        self.fare = fare

    def Fare(self):
        return self.fare

# Create instances of different vehicle types
bus = Vehicle(50)   # Bus fare: $50
car = Vehicle(30)   # Car fare: $30
train = Vehicle(100)  # Train fare: $100
truck = Vehicle(70)  # Truck fare: $70
ship = Vehicle(150)  # Ship fare: $150

# Calculate the total fare for all vehicles
total_fare = bus.Fare() + car.Fare() + train.Fare() + truck.Fare() + ship.Fare()

# Print the total fare
print(f"Total Fare for all vehicles: ${total_fare}")


# Q3. Consider an ongoing test cricket series. Following are the names of the players and their
# scores in the test1 and 2.
# Test Match 1 :
# Dhoni : 56 , Balaji : 94
# Test Match 2 :
# Balaji : 80 , Dravid : 105
# Calculate the highest number of runs scored by an individual cricketer in both of the matches.
# Create a python function Max_Score (M) that reads a dictionary M that recognizes the player
# with the highest total score. This function will return ( Top player , Total Score ) . You can
# consider the Top player as String who is the highest scorer and Top score as Integer .
# Input : Max_Score({‘test1’:{‘Dhoni’:56, ‘Balaji : 85}, ‘test2’:{‘Dhoni’ 87, ‘Balaji’’:200}})
# Output : (‘Balaji ‘ , 200)


def Max_Score(matches):
    top_player = ""
    top_score = 0

    for match_scores in matches.values():
        for player, score in match_scores.items():
            if score > top_score:
                top_player = player
                top_score = score

    return (top_player, top_score)

# Example usage:
matches = {
    'test1': {'Dhoni': 56, 'Balaji': 85},
    'test2': {'Dhoni': 87, 'Balaji': 200}
}

result = Max_Score(matches)
print(result)  # Output: ('Balaji', 200)


# Q4. Create a simple Card game in which there are 8 cards which are randomly chosen from a
# deck. The first card is shown face up. The game asks the player to predict whether the next card
# in the selection will have a higher or lower value than the currently showing card.
# For example, say the card that’s shown is a 3. The player chooses “higher,” and the next card is
# shown. If that card has a higher value, the player is correct. In this example, if the player had
# chosen “lower,” they would have been incorrect. If the player guesses correctly, they get 20
# points. If they choose incorrectly, they lose 15 points. If the next card to be turned over has the
# same value as the previous card, the player is incorrect.



import random

def draw_card():
    # Generate a random card value between 2 and 14 (Ace as 14)
    return random.randint(2, 14)

def play_game():
    score = 0
    previous_card = draw_card()

    print(f"First card: {previous_card}")

    while True:
        guess = input("Predict 'higher' or 'lower': ").lower()

        if guess not in ['higher', 'lower']:
            print("Invalid input. Please enter 'higher' or 'lower'.")
            continue

        current_card = draw_card()
        print(f"Next card: {current_card}")

        if current_card == previous_card:
            print("Same value as previous card. You lose 15 points.")
            score -= 15
        elif (current_card > previous_card and guess == 'higher') or (current_card < previous_card and guess == 'lower'):
            print("Correct! You gain 20 points.")
            score += 20
        else:
            print("Incorrect! You lose 15 points.")
            score -= 15

        print(f"Your score: {score}")
        play_again = input("Play again? (yes/no): ").lower()

        if play_again != 'yes':
            break

        previous_card = current_card

    print(f"Final score: {score}")
    print("Thanks for playing!")

if __name__ == "__main__":
    print("Welcome to the Higher or Lower Card Game!")
    play_game()


# Q5. Create an empty dictionary called Car_0 . Then fill the dictionary with Keys : color , speed
# , X_position and Y_position.
# car_0 = {'x_position': 10, 'y_position': 72, 'speed': 'medium'} .
# a) If the speed is slow the coordinates of the X_pos get incremented by 2.
# b) If the speed is Medium the coordinates of the X_pos gets incremented by 9
# c) Now if the speed is Fast the coordinates of the X_pos gets incremented by 22.
# Print the modified dictionary

# Create an empty dictionary
Car_0 = {}

# Fill the dictionary with keys and initial values
Car_0['color'] = 'red'
Car_0['speed'] = 'medium'
Car_0['X_position'] = 10
Car_0['Y_position'] = 72

# Check the speed and modify 'X_position' accordingly
if Car_0['speed'] == 'slow':
    Car_0['X_position'] += 2
elif Car_0['speed'] == 'medium':
    Car_0['X_position'] += 9
elif Car_0['speed'] == 'fast':
    Car_0['X_position'] += 22

# Print the modified dictionary
print(Car_0)


# Q6. Show a basic implementation of abstraction in python using the abstract classes.
# 1. Create an abstract class in python.
# 2. Implement abstraction with the other classes and base class as abstract class.


from abc import ABC,abstractmethod
class Shape:
    @abstractmethod
    def area(self):
        pass
class Rect(Shape):
    '''The Area of Rectangle 
    '''
    def __init__(self,length,breadth):
        self.length=length
        self.breadth=breadth
    def area(self):
        return(self.length*self.breadth)
c=Rect(2,3)
print(Rect.__doc__)
print(f'Area of Reactangel is {c.area()}')

# Q7. Create a program in python to demonstrate Polymorphism.
# 1. Make use of private and protected members using python name mangling techniques


# Q7. Create a program in python to demonstrate
#  Polymorphism.
# 1. Make use of private and protected members 
# using python name mangling techniques
class Animal:
    def __init__(self, name):
        self._name = name  # Protected member (name mangling)

    def make_sound(self):
        pass

class Dog(Animal):
    def make_sound(self):
        return f"{self._name} barks"

class Cat(Animal):
    def make_sound(self):
        return f"{self._name} meows"

class Cow(Animal):
    def make_sound(self):
        return f"{self._name} moos"


dog = Dog("Buddy")
print(dog.make_sound())
cat = Cat("Whiskers")
print(cat.make_sound())

cow = Cow("Bessie")
print(cow.make_sound())


# animals = [dog, cat, cow]
# for animal in animals:
#     print(animal.make_sound())

# Accessing protected member (name mangling)
print("Protected Member (Name Mangling):", dog._name)


# Q8. Given a list of 50 natural numbers from 1-50. 
# Create a function that will take every element
# from the list and return the square of each element.
#  Use the python map and filter methods to
# implement the function on the given list.


def square(s):
    return s**2
ls1=list(range(1,50))
ls2=list(map(square,ls1))
print(ls2)
ls3=list(filter(square,ls1))
print(ls3)

ls=[(x,x**2) for x in range(0,50)]
print(ls)


# Q9. Create a class, Triangle. Its init() 
# method should take self, angle1, angle2, and 
# angle3 as arguments.

class Triangle:
    def __init__(self, angle1, angle2, angle3):
        self.angle1 = angle1
        self.angle2 = angle2
        self.angle3 = angle3

# Create an instance of the Triangle class
triangle = Triangle(60, 60, 60)

# Access the attributes of the Triangle instance
print("Angle 1:", triangle.angle1)
print("Angle 2:", triangle.angle2)
print("Angle 3:", triangle.angle3)

#Q10. Create a class variable named number_of_sides
# and set it equal to 3.


class Triangle:
    number_of_sides = 3  # Class variable

    def __init__(self, angle1, angle2, angle3):
        self.angle1 = angle1
        self.angle2 = angle2
        self.angle3 = angle3

# Access the class variable
print("Number of sides in a Triangle:", Triangle.number_of_sides)

# Q11. Create a method named check_angles. The sum of a triangle's three angles should return
# True if the sum is equal to 180, and False otherwise. The method should print whether the
# angles belong to a triangle or not.
# 11.1 Write methods to verify if the triangle is an acute triangle or obtuse triangle.
# 11.2 Create an instance of the triangle class and call all the defined methods.
# 11.3 Create three child classes of triangle class - isosceles_triangle, right_triangle and
# equilateral_triangle.
# 11.4 Define methods which check for their properties.

class Triangle:
    def __init__(self, angle1, angle2, angle3):
        self.angle1 = angle1
        self.angle2 = angle2
        self.angle3 = angle3

    def check_angles(self):
        if self.angle1 + self.angle2 + self.angle3 == 180:
            return True
        else:
            return False

    def is_acute_triangle(self):
        if all(angle < 90 for angle in [self.angle1, self.angle2, self.angle3]):
            return True
        else:
            return False

    def is_obtuse_triangle(self):
        if any(angle > 90 for angle in [self.angle1, self.angle2, self.angle3]):
            return True
        else:
            return False

class IsoscelesTriangle(Triangle):
    def is_isosceles(self):
        if self.angle1 == self.angle2 or self.angle1 == self.angle3 or self.angle2 == self.angle3:
            return True
        else:
            return False

class RightTriangle(Triangle):
    def is_right_triangle(self):
        if any(angle == 90 for angle in [self.angle1, self.angle2, self.angle3]):
            return True
        else:
            return False

class EquilateralTriangle(Triangle):
    def is_equilateral(self):
        if self.angle1 == self.angle2 == self.angle3:
            return True
        else:
            return False

# Create an instance of the Triangle class
triangle = Triangle(60, 60, 60)

# Check if the angles belong to a triangle
print("Is it a triangle?", triangle.check_angles())

# Check if it's an acute triangle
print("Is it an acute triangle?", triangle.is_acute_triangle())

# Check if it's an obtuse triangle
print("Is it an obtuse triangle?", triangle.is_obtuse_triangle())

# Create instances of the child classes
isosceles_triangle = IsoscelesTriangle(45, 45, 90)
right_triangle = RightTriangle(30, 60, 90)
equilateral_triangle = EquilateralTriangle(60, 60, 60)

# Check their properties
print("Is it an isosceles triangle?", isosceles_triangle.is_isosceles())
print("Is it a right triangle?", right_triangle.is_right_triangle())
print("Is it an equilateral triangle?", equilateral_triangle.is_equilateral())


# Q12. Create a class isosceles_right_triangle 
# which inherits from isosceles_triangle and
# right_triangle.
# 12.1 Define methods which check for their properties.


class IsoscelesRightTriangle(IsoscelesTriangle, RightTriangle):
    def __init__(self, base, height):
        # Call the constructors of both parent classes
        IsoscelesTriangle.__init__(self, 45, 45, 90)  # Isosceles triangle with 45-degree angles
        RightTriangle.__init__(self, 45, 45, 90)      # Right triangle with 45-degree angles
        self.base = base
        self.height = height

    def is_isosceles_right_triangle(self):
        # Check if it's an isosceles triangle and a right triangle
        return self.is_isosceles() and self.is_right_triangle()

# Create an instance of IsoscelesRightTriangle
isosceles_right_triangle = IsoscelesRightTriangle(4, 4)

# Check if it's an isosceles right triangle
print("Is it an isosceles right triangle?", isosceles_right_triangle.is_isosceles_right_triangle())



















