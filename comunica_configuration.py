import os
import subprocess
import argparse

def handle_ex1():
    # Example: Write to a file
    with open("example1.txt", "w") as f:
        f.write("EX1 was selected.\n")
    print("EX1: example1.txt updated.")

def handle_ex2():
    # Example: Append to a file
    with open("example2.txt", "a") as f:
        f.write("EX2 was selected.\n")
    print("EX2: example2.txt appended.")

def handle_ex3():
    # Example: Create a new file
    with open("example3.txt", "w") as f:
        f.write("EX3 was selected.\n")
    print("EX3: example3.txt created.")

def main():
    user_input = input("Enter EX1, EX2, or EX3: ").strip().upper()
    if user_input == "EX1":
        handle_ex1()
    elif user_input == "EX2":
        handle_ex2()
    elif user_input == "EX3":
        handle_ex3()
    else:
        print("Invalid input. Please enter EX1, EX2, or EX3.")

if __name__ == "__main__":
    main()