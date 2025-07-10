import random

def play():
    user = input("select from rock, paper and scissor:")
    computer = random.choice(['r','p','s'])
    
    if user == computer:
        return computer + " its a tie"

    if is_win(user, computer):
        return computer + ' You won!'

    return computer + ' You Lost'

def is_win(player, opponent):
    if (player == 'r' and opponent == 's') or (player == 'p' and opponent == 'r') or (player == 's' and opponent == 'p'):
        return True

print(play())